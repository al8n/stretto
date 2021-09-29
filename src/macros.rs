#[macro_export]
#[doc(hidden)]
macro_rules! cfg_not_nightly {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "nightly"))]
            #[cfg_attr(docsrs, doc(cfg(not(feature = "nightly"))))]
            $item
        )*
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! cfg_nightly {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "nightly")]
            #[cfg_attr(docsrs, doc(cfg(feature = "nightly")))]
            $item
        )*
    }
}