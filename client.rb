#!/usr/bin/env ruby

require 'socket'
require 'util'
require 'monitor'

$verbosity = 0

class TCPFetchClient
  BYTES_FOR_INIT_RESPONSE = 8
  STATE_NEW = 0
  STATE_CONNECTED = 1
  STATE_INITIAL_REQUEST_SENT = 2
  STATE_INITIAL_REPLY_RCVD_OK = 3

  def initialize(server, port)
    puts "#{$$}: parent constr" if $verbosity >= 1
    begin
      @server = server
      @serverip = IPSocket.getaddress(@server)
    rescue SocketError => e
      $stderr.puts "Cannot get address of #{@server}"
      throw
    end
    @port = port
    retrycount = 0
    begin
      @socket = StatefulTCPSocket.new(@serverip, @port)
      @socket.state = STATE_CONNECTED
    rescue MySocketError => e
      $stderr.puts "Error in connect(): #{e}"
      retry if (retrycount += 1) < 3
    end
  end

  def error (string)
    $stderr.puts string
    throw MySocketError
  end

  def set_file_params(filename, file_size, bytes, start_offset)
    @filename = filename
    @filesize = file_size
    @bytes = (bytes == 0) ? file_size : bytes
    @start_offset = start_offset
  end

  def init_connection(filename, bytes, start_offset = 0)
    puts "#{$$}: parent init connection: #{bytes}, #{start_offset}" if $verbosity >= 1
    retrycount = 0
    str = [filename, bytes, start_offset].join('|')
    str += '|'
    str = str.ljust(512, '0') # pad to 512 bytes
    # puts "[#{str}], #{str.length}"
    begin
      if @socket.state != STATE_CONNECTED
        error("init_params called on unconnected socket") 
      end
      if @socket.send(str,0) != str.length
        error("not able to send all of init_params string")
      end

      # sending worked 
      #puts "sending worked"
      @socket.state = STATE_INITIAL_REQUEST_SENT
      resp = @socket.recvfrom(40) # receive up to 40 bytes
      if resp[0].length == 40 # got 40 bytes indeed
        file_size,regular_file,readable = 
          resp[0].split('|')[0..2].collect{|x| x = x.to_i}
        if (regular_file == 0 ||
            file_size < bytes ||
            file_size < start_offset ||
            readable == 0)
          error ("problem reading remote file")
        end
        @socket.state = STATE_INITIAL_REPLY_RCVD_OK

        set_file_params(filename, file_size, bytes, start_offset)

        #
        # send ack to say we're ready to receive file
        #
        sendack(@socket)

        #
        # receive file
        #
      else
        error("reply received but less than 40 bytes")
      end
    rescue MySocketError,ArgumentError,StandardError
      $stderr.puts "Error in init_params()"
      retry if (retrycount += 1) < 3
    end
  end

  def create_file
    puts "#{$$} parent create_file" if $verbosity >= 1
    fh = File.new(File.basename(@filename), "w")
    return fh
  end

  def receive
    puts "#{$$}: parent receive" if $verbosity >= 2
    retrycount = 0
    begin
      fh = create_file
      transferred_bytes = 0
      bytes_to_read = @bytes
      puts "#{$$}: writing to #{@start_offset}"
      while transferred_bytes < @bytes
        bytes_to_read_thisloop = minimum(bytes_to_read, FILE_TRANSFER_CHUNKSIZE)
        #puts "receiving #{bytes_to_read_thisloop} bytes"
        chunk = @socket.recvfrom(bytes_to_read_thisloop)
        got_bytes = chunk[0].length
        #bytes_to_read -= bytes_to_read_thisloop
        bytes_to_read -= got_bytes
        transferred_bytes += got_bytes
        if got_bytes < bytes_to_read_thisloop
          #error("didnt transfer full chunk but file not ended (got #{got} instead of #{bytes_to_read_thisloop})")
          #$stderr.puts "Error: didnt transfer full chunk but file not ended (got #{got_bytes} instead of #{bytes_to_read_thisloop})"
        end
        if fh.write(chunk[0]) != chunk[0].length
          error("cannot write #{chunk[0].length} bytes to #{fh.path}")
        end
      end
    rescue MySocketError,ArgumentError,StandardError => e
      $stderr.puts "Error in init_params(): #{e}"
      retry if (retrycount += 1) < 3
    ensure
      fh.close if !fh.closed?
    end
  end
end

class MultiTCPFetchClient < TCPFetchClient
  include MonitorMixin
  def initialize(server, port)
    super(server,port)
    puts "#{$$} child constr" if $verbosity >= 2
    @@totchunkstransferred = 0
    @killed = false
  end


  def set_file_params(filename, file_size, bytes, start_offset)
    super(filename, file_size, bytes, start_offset)
    # something like a bittorrent block map, to find chunks we 
    # dont have yet
    #if @@chunkmap.nil?
    #  @@chunkmap = Array.new(file_size/FILE_TRANSFER_CHUNKSIZE, false)
    #end
  end

  def create_file
    puts "#{$$} child create file" if $verbosity >= 2

=begin
    # first make sure the dummy file exists
    if !FileTest.exist?(newfilename) || File.size(newfilename) != $total_file_size
      # file does not exist, so create it
      fh = File.new(newfilename, "w")
      if (fh.flock(File::LOCK_NB|File::LOCK_EX) == 0) 
        # this means we have the exclusive lock
        puts "process #{$$} creating file #{newfilename}"
        remaining = $total_file_size
        towrite = minimum(remaining, 1024)
        while remaining > 0
          fh.write("0"*towrite)
          remaining -= towrite
        end
        puts "#{$$} wrote dummy file of size #{File.size(newfilename)}"
        fh.flock(File::LOCK_UN)
      end
      fh.close
    end
=end

=begin
    fh = File.new($local_filename, "w")
    fh.pos = @start_offset
    puts "#{$$}: setting position = #{@start_offset}"
    return fh
=end

    newfilename = File.basename(@filename)
    fh = File.new("#{newfilename}.#{$$}", "w")
    return fh
  end

=begin
  def get_next_available_chunk
    return [0,0] if @@nextchunkptr > @@chunkmap.size - 1
    synchronize do 
      @@chunkmap[@@nextchunkptr] = true #taken
      chunk_num = @@nextchunkptr
      @@nextchunkptr += 1
    end

    if chunk_num == @@chunkmap.size - 1
      return [chunk_num, file_size % FILE_TRANSFER_CHUNKSIZE]
    else
      return [chunk_num, FILE_TRANSFER_CHUNKSIZE]
    end
  end
=end

  def receive
    puts "#{$$} child receive" if $verbosity >= 2
    #fh = create_file
    #fh.pos = @start_offset
    super()
  end
end


if $0 == __FILE__
  if ARGV.size != 5
    $stderr.puts "Usage: #{__FILE__} <server> <server port> <full path to file> <num simultaneous connections (<20)> <file size in bytes>"
    $stderr.puts "fuck with the options at your own risk!"
    exit
  end
  server = ARGV[0] || "localhost"
  port = ARGV[1] || 1234
  filename = ARGV[2] || "/etc/passwd"
  num_simul_connections = ARGV[3] || 3
  filesize = ARGV[4] || exit
  num_simul_connections = num_simul_connections.to_i
  filesize = filesize.to_i
  $total_file_size = filesize
  file_sizes_and_offsets = Array.new
  cnt = 0
  bytes_per_client = (filesize/num_simul_connections)
  (num_simul_connections-1).times do |i|
    file_sizes_and_offsets << [cnt,bytes_per_client]
    cnt += bytes_per_client
  end
  if (lastchunk = filesize%num_simul_connections) > 0
    file_sizes_and_offsets << [cnt,lastchunk+bytes_per_client]
  else
    file_sizes_and_offsets << [cnt, bytes_per_client]
  end

  puts file_sizes_and_offsets.collect{|x| x.join(',')}.join(' ') if $verbosity >= 3

  # create dummy local file
=begin
  $local_filename = File.basename(filename)
  # first make sure the dummy file exists
  if !FileTest.exist?($local_filename) || File.size($local_filename) != $total_file_size
    # file does not exist, so create it
    fh = File.new($local_filename, "w")
    puts "process #{$$} creating file #{$local_filename}"
    remaining = $total_file_size
    towrite = minimum(remaining, 1024)
    while remaining > 0
      fh.write("0"*towrite)
      remaining -= towrite
    end
    fh.close
    puts "#{$$} wrote dummy file of size #{File.size($local_filename)}"
  end
=end
  

  child_pids = []
  #a = TCPFetchClient.new(server, port)
  time_start = Time.now
  0.upto(num_simul_connections-1) do |i|
    if (pid = fork).nil? # kid process
      a = MultiTCPFetchClient.new(server, port)
      a.init_connection(filename,file_sizes_and_offsets[i][1], file_sizes_and_offsets[i][0])
      a.receive
      exit
    else  # parent
      child_pids << pid
    end
  end
  Process.waitall
  time_end = Time.now

  time_taken = (time_end.to_f - time_start.to_f)
  puts "Tranfer of #{filesize} bytes completed in %10.5f seconds" % time_taken
  if (speed = (filesize.to_f/(time_taken*1024.0)) ) > 1024.0
    speed /= 1024.0
    puts "\tSpeed: %5.2f MBps" % speed
  else
    puts "\tSpeed: %5.2f KBps" % speed
  end

  # merge final files
  finalfh = File.new(File.basename(filename), "w")
  child_pids.each do |pid|
    readfrom = File.open("#{File.basename(filename)}.#{pid}")
    while line = readfrom.gets
      finalfh.print line
    end
    readfrom.close
    File.unlink("#{File.basename(filename)}.#{pid}") 
  end
  finalfh.close
end


=begin
  a = TCPFetchClient.new(server, port)
  a.init_connection(filename, 0, 0)
  a.receive
=end
