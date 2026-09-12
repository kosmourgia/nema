pub mod Nema.Lab.Associated {

    pub trait Backend[t] {
        type Ref
        type View: Type -> Type
        type Aef: Eff
        pub def load(backend: t, ref: Backend.Ref[t]): Backend.View[t][String] \ Backend.Aef[t]
    }

    pub enum FixtureBackend {
        case FixtureBackend
    }

    instance Backend[FixtureBackend] {
        type Ref = String
        type View = List
        type Aef = {}

        pub def load(_backend: FixtureBackend, ref: String): List[String] = ref :: Nil
    }

    pub def loadTwice(backend: t, ref: Backend.Ref[t]):
        (Backend.View[t][String], Backend.View[t][String]) \ Backend.Aef[t] with Backend[t] =
        (Backend.load(backend, ref), Backend.load(backend, ref))
}
