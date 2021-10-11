#[doc(hidden)]
macro_rules! cfg_not_async {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "tokio"))]
            #[cfg_attr(docsrs, doc(cfg(not(feature = "tokio"))))]
            $item
        )*
    }
}

#[doc(hidden)]
macro_rules! cfg_async {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "tokio")]
            #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
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
