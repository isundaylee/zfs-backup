REMOTE_HOST = 'jiahaoli@adp'
REMOTE_CWD = 'backups/nas/zfs'

NC_PORT = 38175
NC_REMOTE_HOST = 'adp'

require_relative 'runners'
require_relative 'utils'

class ZFSDatasetManager
  def initialize(dataset, remote_host, remote_cwd)
    @dataset = dataset
    @remote_host = remote_host
    @remote_cwd = remote_cwd

    @local = LocalRunner.new()
    @remote = SSHRunner.new(remote_host, remote_cwd)
  end

  def backup
    take_local_snapshot

    local_snapshots = list_local_snapshots.sort
    remote_backups = list_remote_backups.sort

    initial_ts = "00000000000000"
    latest = initial_ts
    remote_backups.each do |b|
      from, to = b.split("@")[-1].split("-")
      break unless from == latest
      latest = to
    end

    local_snapshots.drop(1).each do |s|
      ts = s.split("@")[-1]
      break if ts >= latest

      log "Destroying local snapshot #{s}"
      @local.run("sudo zfs destroy #{s}")
    end

    local_snapshots.each do |s|
      ts = s.split("@")[-1]
      next if ts <= latest

      backup_name = "#{latest}-#{ts}"

      if latest == initial_ts
        log "Sending BASE snapshot #{backup_name}"
        @remote.stream_data("sudo zfs send #{s}", "#{backup_name}.in_progress")
      else
        log "Sending INCREMENTAL snapshot #{backup_name}"
        @remote.stream_data("sudo zfs send -i #{@dataset}@#{latest} #{s}", "#{backup_name}.in_progress")
      end

      @remote.run("mv #{backup_name}.in_progress #{backup_name}")
      latest = ts
    end

    log "Finished backing up #{@dataset}. "
  end

  private
    def list_local_snapshots
      @local.run("sudo zfs list -r -H -t snapshot -o name #{@dataset}").lines.map(&:strip)
    end

    def take_local_snapshot
      timestamp = Time.now.strftime("%Y%m%d%H%M%S")
      log "Creating snapshot #{@dataset}/#{timestamp}"
      @local.run("sudo zfs snapshot #{@dataset}@#{timestamp}")
    end

    def list_remote_backups
      @remote.run("ls").lines.map(&:strip).select { |x| x =~ /^\d{14}-\d{14}$/ }
    end
end

[
  "mysql",
  "files",
  "owncloud",
  "time_machine"
].each do |p|
  dataset_manager = ZFSDatasetManager.new("mainpool/#{p}", REMOTE_HOST, File.join(REMOTE_CWD, "mainpool/#{p}"))
  dataset_manager.backup
end

log "Completed back up. "
