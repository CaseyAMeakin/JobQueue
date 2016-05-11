require 'securerandom'

class JobQueue
  
  DEFAULT_CMDS = []
  DEFAULT_IDS  = []
  DEFAULT_NUM_SLOTS = 1
  DEFAULT_LOGFILE = "JobQueue"
  DEFAULT_MONITOR_TIME_INTERVAL_MSEC = 10
  JOB_STATUS = { :done => 'done', :active => 'active', :pending => 'pending'}

  class JobEntry
    attr_accessor :pid, :cmd, :start_time, :stop_time, :status, :uuid, :id
    def initialize(cmd,id)
      @uuid = SecureRandom.uuid
      @cmd = cmd
      @id = id
      @pid = nil
      @start_time = nil
      @stop_time = nil
      @status = JOB_STATUS[:pending]
    end
  end

  attr_accessor :cmds, :num_slots, :run_limits_seconds, :logfile, :monitor_time_interval_msec, :ids
  
  def initialize(args={}) 
    #todo -- argument validation
    @cmds = args[:cmds] || DEFAULT_CMDS
    @ids = args[:ids] || DEFAULT_IDS
    @num_slots = args[:num_slots] || DEFAULT_NUM_SLOTS
    @logfile = args[:logfile] || DEFAULT_LOGFILE
    @monitor_time_interval_msec = args[:monitor_time_interval_msec] || DEFAULT_MONITOR_TIME_INTERVAL_MSEC
    @jobs = []
  end
  
  def start
    initialize_jobs
    consume_jobs
    shutdown
  end
  
  private
  
  def shutdown
    @jobs.each do |job|
      warn "pid=#{job.pid},run_time=#{job.stop_time - job.start_time},cmd=#{job.cmd}"
    end
  end
  
  def consume_jobs
    update_jobs!
    while pending_jobs.count + active_jobs.count > 0
      consume_next! unless pending_jobs.count == 0 || active_jobs.count >= @num_slots
      sleep (@monitor_time_interval_msec/1e3)
      update_jobs!
    end
  end
  
  def update_jobs!
    active_jobs.each do |job|
      unless pid_alive?(job.pid)
        job.stop_time = Time.now
        job.status = JOB_STATUS[:done]
        update_job!(job)
        warn "Finished: pid=#{job.pid}, cmd=#{job.cmd}"
      end
    end
  end
  
  def consume_next!
    return false if pending_jobs.count < 1
    job = pending_jobs.first
    job.pid = fork do
      base_filename = (job.id.class == "".class) ? "#{@logfile}.#{job.id}" : @logfile
      $stdout.reopen("#{base_filename}.#{Process.pid}.#{Time.now.to_i.to_s}.stdout","w")
      $stderr.reopen("#{base_filename}.#{Process.pid}.#{Time.now.to_i.to_s}.stderr","w")
      Process.setsid
      exec(job.cmd)
    end
    warn "Submitted: pid=#{job.pid}, cmd=#{job.cmd}"
    Process.detach(job.pid)
    job.start_time = Time.now
    job.status = JOB_STATUS[:active]
    update_job!(job)
  end
  
  def update_job!(job)
    @jobs = @jobs.select{ |j| j.uuid != job.uuid} + [job]
  end
  
  def initialize_jobs
    (0...@cmds.count).each do  |index|
      @jobs += [ JobEntry.new(@cmds[index],@ids[index]) ]
    end
  end
  
  def pending_jobs
    @jobs.select { |job| job.status == JOB_STATUS[:pending] }
  end
  
  def active_jobs
    @jobs.select { |job| job.status == JOB_STATUS[:active] }
  end
  
  def pid_alive?(pid)
    begin
      Process.getpgid( pid )
      true
    rescue Errno::ESRCH
      false
    end
  end
  
end


# test case / example

if __FILE__ == $0

  my_jobs = JobQueue.new()
  my_jobs.cmds = ['echo sleep 10;sleep 10','echo sleep 3;sleep 3','echo sleep 11;sleep 11']
  my_jobs.num_slots = 2
  my_jobs.logfile = 'application_name'
  my_jobs.start

end


