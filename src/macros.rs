macro_rules! cfg_async {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "async")]
            #[cfg_attr(docsrs, doc(cfg(feature = "async")))]
            $item
        )*
    }
}

macro_rules! cfg_sync {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "sync")]
            #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
            $item
        )*
    }
}

#[doc(hidden)]
macro_rules! cfg_serde {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "serde")]
            #[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
            $item
        )*
    }
}
