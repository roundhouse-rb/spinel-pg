# Lost and closed connections over a scripted transport, CRuby / Spinel parity.
require "pg/client"

class StateTransport
  def initialize(chunks)
    @chunks = chunks
    @index = 0
    @write_result = 0
    @writes = 0
    @closes = 0
  end

  def write(data)
    @writes = @writes + 1
    @write_result
  end

  def read_some(max)
    if @index >= @chunks.length
      return ""
    end
    chunk = @chunks[@index]
    @index = @index + 1
    chunk.b
  end

  def close
    @closes = @closes + 1
    0
  end

  def fail_writes
    @write_result = -1
  end

  def writes
    @writes
  end

  def reads
    @index
  end

  def closes
    @closes
  end
end

def state_check(name, passed)
  raise "failed: " + name unless passed
  puts name + " true"
end

def state_message(kind, body)
  kind + PgWire.be32(body.bytesize + 4) + body.b
end

def state_client(transport)
  client = PgClientCore.new(transport, "u", "d", "", "n")
  client.connect!
  client
end

def state_error(client)
  begin
    client.exec("SELECT 1")
  rescue PG::ConnectionBad => e
    return e.message
  end
  ""
end

nul = [0].pack("C*")
startup = state_message("R", PgWire.be32(0)) + state_message("Z", "I")

t = StateTransport.new([startup])
c = state_client(t)
state_check("lost_raises", state_error(c) == "pg: connection lost")
state_check("lost_state", !c.ready? && c.transaction_status == PG::PQTRANS_UNKNOWN && t.closes == 1)
writes = t.writes
reads = t.reads
state_check("lost_refuses", state_error(c) == "pg: connection is closed" && t.writes == writes && t.reads == reads)
c.close
state_check("lost_close", t.writes == writes && t.closes == 1)

c = state_client(StateTransport.new([startup]))
caught = false
begin
  c.exec("SELECT 1")
rescue PG::Error
  caught = true
end
state_check("lost_is_pg_error", caught)

fatal = state_message("E", "S" + "FATAL" + nul + "V" + "FATAL" + nul + "C" + "57P01" + nul +
                           "M" + "terminating connection due to administrator command" + nul + nul)
t = StateTransport.new([startup, fatal])
c = state_client(t)
shutdown = false
begin
  c.exec("SELECT 1")
rescue PG::AdminShutdown
  shutdown = true
end
state_check("fatal_class", shutdown && !c.ready? && t.closes == 1)
state_check("fatal_refuses", state_error(c) == "pg: connection is closed")

t = StateTransport.new([startup])
c = state_client(t)
t.fail_writes
state_check("write_fails", state_error(c) == "pg: connection lost" && t.reads == 1 && !c.ready? && t.closes == 1)

t = StateTransport.new([startup])
t.fail_writes
c = PgClientCore.new(t, "u", "d", "", "n")
message = ""
begin
  c.connect!
rescue PG::ConnectionBad => e
  message = e.message
end
state_check("write_fails_startup", message == "pg: connection lost" && t.reads == 0)

t = StateTransport.new([startup])
c = state_client(t)
writes = t.writes
c.close
c.close
state_check("close_once", t.writes == writes + 1 && t.closes == 1)
state_check("closed_refuses", state_error(c) == "pg: connection is closed")

c = state_client(StateTransport.new([startup]))
sqlstate = "unset"
begin
  c.exec("SELECT 1")
rescue PG::Error => e
  sqlstate = e.result.error_field(PG::PG_DIAG_SQLSTATE).to_s
end
state_check("lost_result", sqlstate == "")
