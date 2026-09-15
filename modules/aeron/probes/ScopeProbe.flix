import java.util.concurrent.atomic.AtomicInteger
import java.lang.Throwable

pub eff AbortScope { def skip(): Unit }

/// Shows why generic user effects cannot cross native resource cleanup scopes.
def main(): Unit \ IO = {
    let closed = new AtomicInteger(0);
    let result = run {
        let value = try { AbortScope.skip(); 1 }
            catch { case _ex: Throwable => 0 };
        let _ = closed.incrementAndGet();
        value
    } with handler AbortScope {
        def skip(_, _resume) = 99
    };
    println("result=${result}; closeCalls=${closed.get()}")
}
