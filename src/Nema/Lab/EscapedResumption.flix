pub mod Nema.Lab.EscapedResumption {

    pub eff Pause {
        def pause(): Unit
    }

    pub eff ScopeLabel {
        def read(): String
    }

    /// Flix 0.75.3 permits this resumption to be retained in a returned
    /// function. The resumed body still advertises its unhandled ScopeLabel
    /// effect, which is supplied at the later invocation site.
    pub def delayed(): Unit -> String \ ScopeLabel =
        run {
            Pause.pause();
            _ -> ScopeLabel.read()
        } with handler Pause {
            def pause(_, resume) = _ -> resume()()
        }

    pub def invokeUnder(label: String): String = {
        let continuation = delayed();
        run {
            continuation()
        } with handler ScopeLabel {
            def read(_, resume) = resume(label)
        }
    }

    pub def main(): Unit \ IO =
        invokeUnder("later-handler") |> println
}
