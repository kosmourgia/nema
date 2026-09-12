// Nema's temporary bootstrap entry point. Product commands are exposed through
// bin/nema while the daemon boundary is built.
def main(): Unit \ IO =
    println("Nema bootstrap is healthy. Try: ./bin/nema doctor")
