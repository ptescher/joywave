use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use catalog::datafusion::error::Result;

use crate::{CheckpointStorage, JobStorage};

#[derive(Clone)]
pub struct InMemory {
    jobs: Arc<RwLock<HashMap<String, String>>>,
    checkpoints: Arc<RwLock<HashMap<String, u64>>>,
}

impl InMemory {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()).into(),
            checkpoints: RwLock::new(HashMap::new()).into(),
        }
    }
}

#[async_trait::async_trait]
impl JobStorage for InMemory {
    async fn add_job(&self, id: &str, job: &str) -> Result<()> {
        self.jobs
            .write()
            .unwrap()
            .insert(id.to_string(), job.to_string());
        Ok(())
    }

    async fn load_jobs(&self) -> Result<HashMap<String, String>> {
        Ok(self.jobs.read().unwrap().clone())
    }

    async fn cancel_job(&self, job_id: &str) -> Result<()> {
        self.jobs.write().unwrap().remove(job_id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl CheckpointStorage for InMemory {
    async fn save_checkpoint(&self, job_id: &str, position: u64) -> Result<()> {
        self.checkpoints
            .write()
            .unwrap()
            .insert(job_id.to_string(), position);
        Ok(())
    }

    async fn load_checkpoint(&self, job_id: &str) -> Result<Option<u64>> {
        Ok(self.checkpoints.read().unwrap().get(job_id).cloned())
    }
}
