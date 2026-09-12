pub mod Nema.Lab.Continuation {

    use Sys.Env

    pub enum BranchRecipe with Eq, ToString {
        case BranchRecipe(String, String, String, String)
    }

    pub eff ChooseStrategy {
        def pick(): Bool
    }

    /// The computation after `choose` is captured once and resumed twice by
    /// `explore`. Each resumption constructs an independent immutable recipe.
    pub def recipe(parentCheckpoint: String): BranchRecipe \ ChooseStrategy =
        if (ChooseStrategy.pick())
            BranchRecipe.BranchRecipe(parentCheckpoint, "streaming-parser", "compact protocol context", "read-only")
        else
            BranchRecipe.BranchRecipe(parentCheckpoint, "state-machine-parser", "full raw-frame context", "read-only")

    pub def explore(parentCheckpoint: String): List[BranchRecipe] =
        run {
            recipe(parentCheckpoint) :: Nil
        } with handler ChooseStrategy {
            def pick(_, resume) = resume(true) ::: resume(false)
        }

    /// Demonstrates actual order when the handler resumes before performing
    /// its own post-action. Scope-end code is not retroactively chronological.
    pub def orderSensitive(): List[String] =
        run {
            let selected = ChooseStrategy.pick();
            (if (selected) "body:true" else "body:false") :: Nil
        } with handler ChooseStrategy {
            def pick(_, resume) =
                "handler:before" :: (resume(true) ::: ("handler:after" :: Nil))
        }

    pub def render(recipe0: BranchRecipe): String = match recipe0 {
        case BranchRecipe.BranchRecipe(parent, strategy, context, workspace) =>
            "${parent}|${strategy}|${context}|${workspace}"
    }

    pub def main(): Unit \ IO = {
        println("multi-resumption recipes:");
        explore("checkpoint-demo") |> List.map(render) |> List.forEach(println);
        println("handler order:");
        orderSensitive() |> List.forEach(println)
    }

    /// Machine boundary used by the fixture fork interpreter. The parent
    /// checkpoint is chosen outside the effect computation and recorded before
    /// material branch execution.
    pub def emitRecipes(): Unit \ {Env, IO} = {
        let parent = Env.getVar("NEMA_PARENT_CHECKPOINT") |> Option.getWithDefault("checkpoint-unset");
        explore(parent) |> List.map(render) |> List.forEach(println)
    }
}
