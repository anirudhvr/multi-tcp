#!/usr/bin/env ruby

require 'socket'
require 'util'
include Socket::Constants

LISTEN_PORT = 1234
LISTEN_QUEUE_LENGTH = 20

class  TCPFetchServer
  STATE_LISTENING = 0
  STATE_CONNECTION_RECEIVED = 1
  STATE_INITINFO_SENT = 2
  STATE_TRANSFER_STARTED = 3

  def error (string)
    $stderr.puts string
    throw MySocketError
  end

  def initialize(port)
    retrycount = 0
    begin
      @port = port
      @serv = StatefulTCPServer.new("0.0.0.0", @port)
=begin
      sockaddr = Socket.pack_sockaddr_in(LISTEN_PORT, 'localhost')
      if @serv.bind(sockaddr) != 0
        error("Cannot bind to port #{LISTEN_PORT}")
      end
      if @serv.listen(LISTEN_QUEUE_LENGTH) != 0
        error("Cannot listen on port #{LISTEN_PORT} for some reason")
      end
=end
      @state = STATE_LISTENING
      @connected_pids = []
    rescue MySocketError => e
      $stderr.puts("Error: #{e}")
      retry if (retrycount += 1) < 3
    end
  end

  def check_and_send_chunks(sock)
      resp = sock.recvfrom(512)
      got_len = resp[0].length
      #puts "got #{resp[0]}"
      if (params = resp[0].split('|')).size != 4 # got 3 params as we wanted
        error("cannot service request from #{sock.to_s} for #{resp[0]}")
      end
      toreturn = []
      if FileTest.exist?(params[0]) # make sure file exists
        toreturn << File.stat(params[0]).size.to_s
        toreturn << (File.stat(params[0]).file? ? "1" : "0")
        toreturn << (File.stat(params[0]).readable? ? "1" : "0")
        tosend = toreturn.join('|')
        error("response too big to send: #{tosend.size}") if tosend.size >= 40
        tosend += '|'
        tosend = tosend.ljust(40, '0')
        if sock.send(tosend,0) == tosend.length
          @state = STATE_INITINFO_SENT
        else
          error("could not send whole init info")
        end
      else
        error("File #{params[0]} does not exist")
      end

      bytes_sought = params[1].to_i
      bytes_sought = File.size(params[0]) if bytes_sought == 0
      start_offset = params[2].to_i
      if ( (start_offset + bytes_sought) > File.size(params[0]))
        error("cannot return #{bytes_sought} from #{start_offset} for file #{params[0]}")
      end

      resp = sock.recvfrom(10)
      if is_ack?(resp[0]) # client asks to start data transfer
        # start sending file in chunks of FILE_TRANSFER_CHUNKSIZE
        file = File.new(params[0], "r")
        file.seek(start_offset)
        count_chunks = 0
        str = ""
        bytes_to_read = bytes_sought
        bytes_to_read_thisloop = minimum(bytes_to_read, FILE_TRANSFER_CHUNKSIZE)
        while bytes_to_read > 1 && file.read(bytes_to_read_thisloop, str)
          count_chunks += 1
          #puts "sending #{bytes_to_read_thisloop} bytes, #{bytes_to_read} left"
          tosend_length = str.length
          while (sent = sock.send(str,0)) < tosend_length
            #error("Could not send the whole str (#{sent} instead of #{str.length}) in the #{count_chunks}th epoch")
            str.slice!(0,sent)
          end
          bytes_to_read -= bytes_to_read_thisloop
        end
      end
  end

  def service(sock)
    begin
      check_and_send_chunks(sock)
    rescue MySocketError => e
      return 1
    ensure
      sock.close
    end
    return 0
  end

  def service_connections
    while true
      retrycount = 0
      begin
        clientsock = @serv.accept
        puts "Got connection from: #{clientsock.peeraddr.join(", ")} (curr q length: #{@connected_pids.size})"
        if @connected_pids.size >= LISTEN_QUEUE_LENGTH
          cpid, status = Process.wait2 # wait for a kid to die
          @connected_pids -= [cpid]
          error("wtf!") if (@connected_pids.size >= LISTEN_QUEUE_LENGTH) 
        end

        pid = fork
        if pid.nil? # child
          @state = STATE_CONNECTION_RECEIVED
          exit service(clientsock)
        else # parent
          @connected_pids << pid
        end
      rescue => e
        $stderr.puts "Unknown error #{e}"
      rescue MySocketError => e
        retry if (retrycount += 1) < 3
      end
    end
  end
end


if $0 == __FILE__
  port = ARGV[0] || 1234
  port = port.to_i
  serv = TCPFetchServer.new(port)
  serv.service_connections
end

