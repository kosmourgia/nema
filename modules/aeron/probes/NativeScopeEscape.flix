use Nema.Aeron

pub eff Escape { def leave(): Unit }

/// Deliberately rejected: Escape cannot cross the IO-only native scope.
def forbidden(): Result[Aeron.Failure, Unit] \ {IO, Escape} =
    Aeron.withDriver("unused-no-runtime", _ -> { Escape.leave(); Ok(()) })
