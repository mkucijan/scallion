use redis::streams::{StreamMaxlen, StreamReadOptions};
use redis::Cmd;

#[derive(Debug, Clone)]
pub struct TaskStream {
    prefix: String,
    group: String,
    consumer_group: String,
    stream: String,
    broadcast_channel: String,
    consumer: String,
    max_stream_size: usize,
}

impl TaskStream {
    pub fn new(prefix: String, group: String, consumer: String) -> Self {
        Self {
            consumer_group: format!("{prefix}_{group}"),
            stream: format!("{prefix}_scallion_task_stream"),
            broadcast_channel: format!("{prefix}_scallion_task_stream_broadcast"),
            consumer,
            max_stream_size: 100_000,
            prefix,
            group,
        }
    }

    pub fn prefix(&self) -> &str {
        self.prefix.as_str()
    }

    pub fn group(&self) -> &str {
        self.group.as_str()
    }

    pub fn consumer(&self) -> &str {
        self.group.as_str()
    }

    pub fn task_result_key(&self, task_name: &str, id: &str) -> String {
        format!("{}_storage:{}:{}", self.stream, task_name, id)
    }

    pub fn xgroup_create_mkstream(&self) -> Cmd {
        Cmd::xgroup_create_mkstream(&self.stream, &self.consumer_group, "$")
    }

    pub fn xgroup_create_mkstream_broadcast(&self) -> Cmd {
        // Use consumer name as consumer group
        Cmd::xgroup_create_mkstream(&self.broadcast_channel, &self.consumer, "$")
    }

    pub fn xadd(&self, maxsize: Option<usize>, data: &[(String, Vec<u8>)]) -> Cmd {
        let maxsize = maxsize.unwrap_or(self.max_stream_size);
        let maxsize = maxsize.max(self.max_stream_size);
        Cmd::xadd_maxlen(&self.stream, StreamMaxlen::Approx(maxsize), "*", data)
    }

    pub fn xreadgroup(&self, timeout: usize, count: usize, ids: &[String]) -> Cmd {
        let opts = StreamReadOptions::default()
            .block(timeout)
            .count(count)
            .group(self.consumer_group.as_str(), self.consumer.as_str());
        Cmd::xread_options(&[self.stream.as_str()], ids, &opts)
    }

    pub fn xadd_broadcast(&self, maxsize: Option<usize>, data: &[(String, Vec<u8>)]) -> Cmd {
        let maxsize = maxsize.unwrap_or(self.max_stream_size);
        let maxsize = maxsize.max(self.max_stream_size);
        Cmd::xadd_maxlen(
            &self.broadcast_channel,
            StreamMaxlen::Approx(maxsize),
            "*",
            data,
        )
    }

    pub fn xread_broadcast(&self, timeout: usize) -> Cmd {
        let opts = StreamReadOptions::default()
            .block(timeout)
            .count(self.max_stream_size)
            .group(self.consumer.as_str(), self.consumer.as_str());
        Cmd::xread_options(&[self.broadcast_channel.as_str()], &[">"], &opts)
    }

    pub fn xack(&self, ids: &[String]) -> Cmd {
        Cmd::xack(&self.stream, &self.consumer_group, ids)
    }

    pub fn set_task_result(
        &self,
        task_name: &str,
        id: &str,
        value: Vec<u8>,
        timeout: usize,
    ) -> Cmd {
        Cmd::set_ex(self.task_result_key(task_name, id), value, timeout)
    }

    pub fn get_task_result(&self, task_name: &str, id: &str) -> Cmd {
        Cmd::get(self.task_result_key(task_name, id))
    }

    pub fn xpending_consumer(&self) -> Cmd {
        Cmd::xpending_consumer_count(
            &self.stream,
            &self.consumer_group,
            "-",
            "+",
            self.max_stream_size,
            &self.consumer,
        )
    }

    pub fn xclaim(&self, ids: &[String]) -> Cmd {
        Cmd::xclaim(&self.stream, &self.consumer_group, &self.consumer, 0, ids)
    }
}
