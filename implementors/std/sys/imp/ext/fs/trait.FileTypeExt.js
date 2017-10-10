(function() {var implementors = {};
implementors["bytes"] = [];
implementors["libc"] = [];
implementors["mio"] = [];
implementors["owning_ref"] = [];
implementors["parking_lot"] = [];
implementors["parking_lot_core"] = [];
implementors["regex_syntax"] = [];
implementors["thread_local"] = [];
implementors["tokio_core"] = [];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
