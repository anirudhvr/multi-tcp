#!/usr/bin/env ruby

FILE_TRANSFER_CHUNKSIZE = 4096 # in bytes

class MySocketError < SocketError
end

class StatefulTCPSocket < TCPSocket
    def initialize(hostname, port)
        @state = 0
        super(hostname, port)
    end

    attr_accessor :state
end

def sendack(sock)
    ack = "ACK"
    ack = ack.ljust(10,'0')
    sock.send(ack,0)
end

def is_ack?(str)
    return (str =~ /^ACK/) ? true : false
end

def minimum(a,b)
  if a < b 
    return a
  else
    return b
  end
end

class StatefulTCPServer < TCPServer
    def initialize(hostname, port)
        @state = 0
        super(hostname, port)
    end

    attr_accessor :state
end
