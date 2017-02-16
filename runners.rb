require 'open3'

class CommandRuntimeError < RuntimeError; end

class Runner
  def run
    raise CommandRuntimeError, "Not implemented yet. "
  end
end

class LocalRunner < Runner
  def run(cmd)
    out, err, code = Open3.capture3("#{cmd}")
    raise CommandRuntimeError, "[#{@host}] #{err}" unless code == 0
    return out
  end
end

class SSHRunner < Runner
  def initialize(host, cwd)
    @host = host
    @cwd = cwd
  end

  def run(cmd)
    out, err, code = Open3.capture3("ssh #{@host} 'mkdir -p \"#{@cwd}\" && cd \"#{@cwd}\" && #{cmd}'")
    raise CommandRuntimeError, "[#{@host}] #{err}" unless code == 0
    return out
  end

  def stream_data(local_command, remote_filename)
    out, err, code = Open3.capture3("#{local_command} | ssh -c arcfour #{@host} 'mkdir -p \"#{@cwd}\" && cd \"#{@cwd}\" && cat > #{remote_filename}'")
    raise CommandRuntimeError, "[#{@host}] #{err}" unless code == 0
    return out
  end
end
