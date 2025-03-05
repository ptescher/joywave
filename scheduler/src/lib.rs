use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use catalog::datafusion::arrow::array::RecordBatch;
use catalog::datafusion::error::DataFusionError;
use catalog::datafusion::execution::RecordBatchStream;
use catalog::datafusion::prelude::*;
use futures::StreamExt;

pub mod storage;

pub type Result<T> = std::result::Result<T, DataFusionError>;

#[derive(Clone)]
pub struct CheckpointManager {
    // Store the last successfully processed position
    pub storage: Arc<Box<dyn CheckpointStorage>>,
}

impl CheckpointManager {
    pub fn new<S: CheckpointStorage + 'static>(storage: S) -> Self {
        Self {
            storage: Arc::new(Box::new(storage)),
        }
    }
}

#[async_trait::async_trait]
pub trait CheckpointStorage: Send + Sync {
    async fn save_checkpoint(&self, job_id: &str, position: u64) -> Result<()>;
    async fn load_checkpoint(&self, job_id: &str) -> Result<Option<u64>>;
}

#[async_trait::async_trait]
pub trait JobStorage: Send + Sync {
    async fn add_job(&self, id: &str, sql: &str) -> Result<()>;
    async fn cancel_job(&self, id: &str) -> Result<()>;
    async fn load_jobs(&self) -> Result<HashMap<String, String>>;
}

#[derive(Clone)]
pub struct Job {
    pub id: String,
    pub sql: String,
    pub checkpoint_manager: CheckpointManager,
}

impl Job {
    pub fn new(id: String, sql: String, checkpoint_manager: CheckpointManager) -> Self {
        Self {
            id,
            sql,
            checkpoint_manager,
        }
    }
}

impl Eq for Job {}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for Job {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

pub struct Scheduler {
    ctx: SessionContext,
    jobs: Arc<RwLock<HashSet<Job>>>,
    checkpoint_manager: CheckpointManager,
    job_storage: Arc<Box<dyn JobStorage>>,
    streams: Arc<RwLock<HashMap<String, Pin<Box<dyn RecordBatchStream + Send>>>>>,
}

impl Scheduler {
    pub fn new<S: JobStorage + 'static>(
        ctx: SessionContext,
        job_storage: S,
        checkpoint_manager: CheckpointManager,
    ) -> Self {
        Self {
            ctx,
            jobs: Arc::new(RwLock::new(HashSet::new())),
            checkpoint_manager,
            job_storage: Arc::new(Box::new(job_storage)),
            streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn execute(&self, id: &str, sql: &str) -> Result<()> {
        self.job_storage.add_job(id, sql).await?;

        let job = Job {
            id: id.to_string(),
            sql: sql.to_string(),
            checkpoint_manager: self.checkpoint_manager.clone(),
        };

        self.jobs.write().unwrap().insert(job);

        Ok(())
    }

    async fn init(&self) -> Result<()> {
        let jobs = self.job_storage.load_jobs().await?;
        for (id, sql) in jobs {
            self.jobs.write().unwrap().insert(Job {
                id,
                sql,
                checkpoint_manager: self.checkpoint_manager.clone(),
            });
        }
        Ok(())
    }

    pub async fn run(&self, callback: impl Fn(RecordBatch) -> ()) -> Result<()> {
        self.init().await?;

        let jobs = self.jobs.read().unwrap();
        let ctx = self.ctx.clone();

        let stream = futures::stream::iter(jobs.iter()).then(|job| {
            let ctx = ctx.clone();
            async move { execute_job(ctx.clone(), job).await }
        });

        futures::pin_mut!(stream);

        while let Some(job) = stream.next().await {
            match job {
                Ok((job, stream)) => {
                    self.streams.write().unwrap().insert(job.id, stream);
                }
                Err(error) => log::error!("Job failed: {error}"),
            }
        }

        loop {
            let mut streams: std::sync::RwLockWriteGuard<
                '_,
                HashMap<String, Pin<Box<dyn RecordBatchStream + Send>>>,
            > = self.streams.write().unwrap();

            if streams.is_empty() {
                return Ok(());
            }

            let mut completed_jobs = Vec::new();

            for (job_id, stream) in streams.iter_mut() {
                if let Some(result) = stream.next().await {
                    match result {
                        Ok(batch) => {
                            callback(batch);
                        }
                        Err(error) => log::error!("Job {} failed: {error}", job_id),
                    }
                } else {
                    completed_jobs.push(job_id.clone());
                }
            }

            drop(streams);

            for job_id in completed_jobs {
                self.complete_job(&job_id).await?;
            }
        }
    }

    async fn complete_job(&self, job_id: &str) -> Result<()> {
        self.job_storage.cancel_job(&job_id).await?;

        let mut streams: std::sync::RwLockWriteGuard<
            '_,
            HashMap<String, Pin<Box<dyn RecordBatchStream + Send>>>,
        > = self.streams.write().unwrap();

        streams.remove(job_id);

        Ok(())
    }
}

async fn execute_job(
    ctx: SessionContext,
    job: &Job,
) -> Result<(Job, Pin<Box<dyn RecordBatchStream + Send>>)> {
    let data_frame = ctx.sql(&job.sql).await?;
    let stream = data_frame.execute_stream().await?;
    Ok((job.clone(), stream))
}
