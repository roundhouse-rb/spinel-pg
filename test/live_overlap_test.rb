# Waits on the server overlap on one worker, spinel-only (committed
# snapshot), same shape as live_extended_test. Four threads each run
# pg_sleep(0.3) on their own connection with SPINEL_WORKERS=1: a wait
# that held the worker would take 1.2 s, one that parks only its green
# thread takes 0.3 s.
require "pg"

ENV["SPINEL_WORKERS"] = "1"

module OvShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

def ov_connect_retry(port, db, user, password)
  attempts = 0
  while true
    begin
      c = PG.connect("127.0.0.1", port, db, user, password)   # matz/spinel#1775: assign-then-return
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "live: postgres did not come up on " + port.to_s
      end
      OvShell.sp_net_shell_capture("sleep 0.2", 16)
    end
  end
end

DIR = "/tmp/spinel-pg-live-overlap"
STOP = "pg_ctl -D " + DIR + " stop -m immediate >/dev/null 2>&1; " +
       "pg_ctl -D " + DIR + " status >/dev/null 2>&1; s=$?; " +
       "if [ $s -eq 3 ] || [ $s -eq 4 ]; then rm -rf " + DIR + " 2>/dev/null && echo ov-stopped; fi"
OvShell.sp_net_shell_capture(STOP, 16)
up = OvShell.sp_net_shell_capture(
  "if ! out=$(initdb -D " + DIR + " -U spinel_test --auth=trust -E UTF8 --locale=C -N 2>&1); then echo \"$out\" | tail -5; " +
  "elif pg_ctl -D " + DIR + " -o '-p 16454 -c listen_addresses=127.0.0.1 -c unix_socket_directories=" + DIR + "'" +
  " -l " + DIR + "/log -w start >/dev/null 2>&1; then echo ov-started; else tail -5 " + DIR + "/log; fi", 4096)
if !up.to_s.include?("ov-started")
  OvShell.sp_net_shell_capture(STOP, 16)
  raise "live: postgres did not start on 16454\n" + up.to_s
end

begin
  conns = []
  4.times { conns.push(ov_connect_retry(16454, "postgres", "spinel_test", "")) }
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  threads = []
  conns.each { |c| threads.push(Thread.new { c.exec("SELECT pg_sleep(0.3)") }) }
  threads.each { |t| t.join }
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
  puts "overlapped   " + (elapsed < 0.9).to_s
  conns.each { |c| c.close }
ensure
  down = OvShell.sp_net_shell_capture(STOP, 16)
  if !down.to_s.include?("ov-stopped")
    puts "teardown     failed; check " + DIR
  end
end
puts "done"
