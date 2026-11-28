# Lost and closed connections over a scripted transport, CRuby / Spinel parity.
require "pg/client"

class StateTransport
  def initialize(chunks)
    @chunks = chunks
    @index = 0
    @write_result = 0
    @writes = 0
    @closes = 0
    @raise_on_read = false
  end

  def write(data)
    @writes = @writes + 1
    @write_result
  end

  def read_some(max)
    if @raise_on_read
      raise "transport read failed"
    end
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

  def fail_reads_with_raise
    @raise_on_read = true
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
state_check("write_fails_startup", message == "pg: connection lost" && t.reads == 0 && !c.ready? && t.closes == 1)

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

def state_fatal(code, message)
  nul = [0].pack("C*")
  state_message("E", "S" + "FATAL" + nul + "V" + "FATAL" + nul + "C" + code + nul + "M" + message + nul + nul)
end

def state_closed_after_failure?(client, transport)
  return false if client.ready?
  return false unless client.transaction_status == PG::PQTRANS_UNKNOWN
  return false unless transport.closes == 1
  writes = transport.writes
  reads = transport.reads
  return false unless state_error(client) == "pg: connection is closed"
  return false unless transport.writes == writes && transport.reads == reads
  client.close
  transport.writes == writes && transport.closes == 1
end

t = StateTransport.new([startup])
c = PgClientCore.new(t, "u", "d", "", "n")
state_check("connect_returns_zero", c.connect! == 0 && c.ready? && t.closes == 0)

t = StateTransport.new([startup])
c = state_client(t)
writes = t.writes
c.lost!
c.lost!
c.close
state_check("lost_idempotent", !c.ready? && c.transaction_status == PG::PQTRANS_UNKNOWN &&
                               t.writes == writes && t.closes == 1)

# Startup FATAL errors keep their SQLSTATE class and close the transport once.
t = StateTransport.new([state_message("R", PgWire.be32(3)), state_fatal("28P01", "password authentication failed")])
c = PgClientCore.new(t, "u", "d", "bad", "n")
ok = false
begin
  c.connect!
rescue PG::InvalidPassword => e
  ok = e.result.error_field(PG::PG_DIAG_SQLSTATE) == "28P01"
end
retry_msg = ""
writes_before_retry = t.writes
begin
  c.connect!
rescue PG::ConnectionBad => e
  retry_msg = e.message
end
state_check("startup_fatal_bad_password", ok && state_closed_after_failure?(c, t) &&
                                          retry_msg == "pg: connection is closed" && t.writes == writes_before_retry)

scram_sasl = state_message("R", PgWire.be32(10) + "SCRAM-SHA-256" + nul + nul)
scram_first = state_message("R", PgWire.be32(11) + "r=n_srv,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=1")
t = StateTransport.new([scram_sasl, scram_first, state_fatal("28P01", "password authentication failed")])
c = PgClientCore.new(t, "u", "d", "bad", "n")
ok = false
begin
  c.connect!
rescue PG::InvalidPassword => e
  ok = e.result.error_field(PG::PG_DIAG_SQLSTATE) == "28P01"
end
state_check("startup_fatal_scram_bad_password", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_fatal("3D000", "database \"missing\" does not exist")])
c = PgClientCore.new(t, "u", "missing", "", "n")
ok = false
begin
  c.connect!
rescue PG::InvalidCatalogName => e
  ok = e.result.error_field(PG::PG_DIAG_SQLSTATE) == "3D000"
end
state_check("startup_fatal_missing_db", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_fatal("57P03", "the database system is starting up")])
c = PgClientCore.new(t, "u", "d", "", "n")
ok = false
begin
  c.connect!
rescue PG::OperatorIntervention => e
  ok = e.result.error_field(PG::PG_DIAG_SQLSTATE) == "57P03"
end
state_check("startup_fatal_starting_up", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_fatal("53300", "sorry, too many clients already")])
c = PgClientCore.new(t, "u", "d", "", "n")
ok = false
begin
  c.connect!
rescue PG::InsufficientResources => e
  ok = e.result.error_field(PG::PG_DIAG_SQLSTATE) == "53300"
end
state_check("startup_fatal_too_many_conns", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_message("R", PgWire.be32(0)), state_fatal("57P01", "terminating connection")])
c = PgClientCore.new(t, "u", "d", "", "n")
ok = false
begin
  c.connect!
rescue PG::AdminShutdown => e
  ok = e.result.error_field(PG::PG_DIAG_SQLSTATE) == "57P01"
end
state_check("startup_fatal_after_auth_ok", ok && state_closed_after_failure?(c, t))

# EOF during startup closes once (no double close from read_msg + ensure).
t = StateTransport.new([])
c = PgClientCore.new(t, "u", "d", "", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = e.message == "pg: connection lost" && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_eof", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_message("R", PgWire.be32(0))])
c = PgClientCore.new(t, "u", "d", "", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = e.message == "pg: connection lost" && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_eof_after_auth_ok", ok && state_closed_after_failure?(c, t))

# Client-side auth and SCRAM rejections raise PG::ConnectionBad and close once.
t = StateTransport.new([state_message("R", PgWire.be32(10) + "PLAIN" + nul + nul)])
c = PgClientCore.new(t, "u", "d", "p", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = !e.is_a?(RuntimeError) && e.message.include?("PLAIN") && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_unsupported_sasl", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([scram_sasl, state_message("R", PgWire.be32(11) + "garbage")])
c = PgClientCore.new(t, "u", "d", "p", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = !e.is_a?(RuntimeError) && e.message.include?("malformed") && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_scram_malformed", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([scram_sasl, state_message("R", PgWire.be32(11) + "r=wrong,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=1")])
c = PgClientCore.new(t, "u", "d", "p", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = !e.is_a?(RuntimeError) && e.message.include?("nonce") && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_scram_bad_nonce", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([scram_sasl, scram_first, state_message("R", PgWire.be32(12) + "v=bad")])
c = PgClientCore.new(t, "u", "d", "p", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = !e.is_a?(RuntimeError) && e.message.include?("signature") && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_scram_bad_sig", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_message("R", PgWire.be32(5) + "salt")])
c = PgClientCore.new(t, "u", "d", "p", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = !e.is_a?(RuntimeError) && e.message.include?("md5") && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_md5_unsupported", ok && state_closed_after_failure?(c, t))

t = StateTransport.new([state_message("R", PgWire.be32(99))])
c = PgClientCore.new(t, "u", "d", "p", "n")
ok = false
begin
  c.connect!
rescue PG::ConnectionBad => e
  ok = !e.is_a?(RuntimeError) && e.message.include?("99") && e.result.error_field(PG::PG_DIAG_SQLSTATE).nil?
end
state_check("startup_unsupported_auth_code", ok && state_closed_after_failure?(c, t))

# Any unexpected raise during startup still closes the transport once.
t = StateTransport.new([])
t.fail_reads_with_raise
c = PgClientCore.new(t, "u", "d", "", "n")
ok = false
begin
  c.connect!
rescue RuntimeError => e
  ok = e.message == "transport read failed"
end
state_check("startup_read_raises", ok && state_closed_after_failure?(c, t))
