# UpRight is demonstration quality code. We do not recommend deploying important services on it (yet). We're working to improve the code. Help us by trying UpRight with your application and telling us how it goes. #

# TBD #

  1. Dynamic client membership
  1. Dynamic server membership
  1. Update MAC/signatures (replace MACs over time, related to dynamic membership above)
  1. Support multiple outstanding requests per client
  1. Support larger numbers of clients (currently cap around 512 due to socket issues)


# Known Issues #
  1. Failure Detection
    * Current behavior is throw an exception
    * Need to add "deployment mode" that logs failures and continues to run
  1. View Change
    * Is a "everybody is correct" stub.  needs to be transitioned to implement full BFT view definition
    * Viewchangeack message definition needs to be updated
    * Does not work with r < u
  1. Stable storage
    * We currently record to stable storage everywhere
  1. Kill/restart server experiment fails for readonly workload.