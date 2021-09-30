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

#[macro_export]
#[doc(hidden)]
macro_rules! cfg_not_serde {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "serde"))]
            #[cfg_attr(docsrs, doc(cfg(not(feature = "serde"))))]
            $item
        )*
    }
}

#[macro_export]
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
