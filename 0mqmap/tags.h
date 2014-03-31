#ifndef _TAGS_50C8919F7B4A_
#define _TAGS_50C8919F7B4A_

enum JobTags {
  JOB_BUSY = 1,
  JOB_DONE = 2,
  JOB_QUEUED = 3,
  JOB_WAIT = 4
};

enum QueueTags {
  QUEUE_HEARTBEAT_TAG = 1,
  QUEUE_JOB_TAG = 2
};

enum WorkerTags {
  WORKER_HEARTBEAT_TAG = 1,
  WORKER_UPDATE_TAG = 2
};

#endif /* _TAGS_50C8919F7B4A_ */
