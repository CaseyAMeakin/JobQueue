# JobQueue

### Description and Usage


This example queues up the system commands listed in the ```@cmds``` instance variables and keeps ```@num_slots``` of them running until they have all completed.

```
my_jobs = JobQueue.new()
my_jobs.cmds = ['echo sleep 10;sleep 10','echo sleep 3;sleep 3','echo sleep 11;sleep 11']
my_jobs.num_slots = 2
my_jobs.logfile = 'application_name'
my_jobs.start

```

