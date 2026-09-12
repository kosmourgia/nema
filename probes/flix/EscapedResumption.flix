/// Bounded research probe for Flix 0.75.3. This file is intentionally outside
/// src/ so an unsupported experiment cannot break the product build.
pub mod EscapedResumption {

    pub eff Pause {
        def pause(): Unit
    }

    pub eff ScopeLabel {
        def read(): String
    }

    /// Attempts to retain a resumption inside a returned function and invoke it
    /// after the Pause handler clause has returned.
    pub def delayed(): Unit -> String \ ScopeLabel =
        run {
            Pause.pause();
            _ -> ScopeLabel.read()
        } with handler Pause {
            def pause(_, resume) = _ -> resume()()
        }

    pub def main(): Unit \ IO = {
        let continuation = delayed();
        let result = run {
            continuation()
        } with handler ScopeLabel {
            def read(_, resume) = resume("later-handler")
        };
        println(result)
    }
}
