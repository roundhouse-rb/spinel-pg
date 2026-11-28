# The real transport: TCP to a PostgreSQL server over sp_net, or its
# unix socket when host is a directory, as in libpq. Same duck as the
# scripted test transport (write / read_some / close).
require "pg/sock"
require "pg/errors"

class PgTransport
  def initialize(host, port)
    if host.start_with?("/")
      path = host + "/.s.PGSQL." + port.to_s
      @fd = PgSock.sp_net_unix_connect(path)
    else
      path = host + ":" + port.to_s
      @fd = PgSock.sp_net_connect(host, port)
    end
    if @fd < 0
      raise PG::ConnectionBad.new("pg: cannot connect to " + path, PgErrorResult.new(""))
    end
  end

  def fd
    @fd
  end

  def write(data)
    PgSock.sp_net_write_bytes(@fd, data, data.bytesize)
  end

  def read_some(max)
    PgSock.sp_net_recv_some(@fd, max)
  end

  def close
    PgSock.sp_net_close(@fd)
  end
end
