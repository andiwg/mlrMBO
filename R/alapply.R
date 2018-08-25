library(parallel)

startJob = function(coreid, sched.info, X, FUN){
  if(any(sched.info$ava)){
    if(any(sched.info$ava[,coreid])){
      # job ava
      jobid <- which(sched.info$ava[,coreid])[1]
    } else {
      # steal job from other core
      ava = sapply(X = seq_along(sched.info$ava[,1]), FUN = function (i) any(sched.info$ava[i,]))
      jobid = which.max(ava)
    }
    sched.info$ava[jobid,] = FALSE
    sched.info$job.id[coreid] = jobid
    waiting = which(sched.info$waiting == jobid)
    if (length(waiting) == 1){
      # blockierter Job
      sched.info$jobs[coreid] = sched.info$waiting.jobs[waiting]
      #sched.info$waiting[waiting] = NA
      #sched.info$waiting.jobs[[waiting]] = NA
      sched.info$wtime[waiting] = proc.time()[3] - sched.info$wtime[waiting]
      sched.info$job.pid[coreid] = sched.info$jobs[[coreid]]$pid
      system(paste0("kill -CONT ", sched.info$job.pid[coreid]), intern = TRUE)
    }else{
      # neuer Job
      sched.info$jobs[coreid] = list(mcparallel(FUN(X[[jobid]]),
                                                mc.set.seed = TRUE,
                                                silent = FALSE))
      sched.info$job.pid[coreid] = sched.info$jobs[[coreid]]$pid
    }
    write(paste0("new Job: ", jobid, " pid: ", sched.info$job.pid[coreid]  ), file = "scheduling", append = TRUE)
  }
  return(sched.info)
}


alapply = function (X, scheduled.on, wait.at, FUN){
  messagef("start Job Execution")
  write(paste0("start Execution"), file = "scheduling", append = TRUE)
  mccollect(wait = FALSE, timeout = 1)
  sched.info = list()
  sx <- seq_along(X)
  res <- vector("list", length(sx))
  
  hcpu = max(unlist(x = scheduled.on,recursive = TRUE))
  cpu.map= lapply(sx, function (i){
    data = vector(mode= "logical",length = hcpu)
    data[as.vector(scheduled.on[[i]])] = TRUE
    data
  })
  sched.info$ava = do.call(rbind,cpu.map)# Jobs bereit zum starten
  fin <- rep(FALSE, length(X))# Jobs fertig?
  sched.info$job.id = numeric(hcpu)#Enthaelt die Job Id der gerade laufenden Jobs
  # choose first item for each core to start
  # delete unused cores off the matrix and 
  for (i in 1:hcpu){
    sched.info$job.id[i] = which(sched.info$ava[,i])[1]
    sched.info$ava[sched.info$job.id[i],] = FALSE
  }
  unusedCores = which(is.na(sched.info$job.id))
  sched.info$job.id = na.omit(sched.info$job.id)
  if (length(unusedCores) > 0) sched.info$ava = sched.info$ava[,-unusedCores, drop = FALSE]
  sched.info$jobs <- lapply(sched.info$job.id,
                            function(i) mcparallel(FUN(X[[i]]),
                                                   mc.set.seed = TRUE,
                                                   silent = FALSE))#, mc.affinity = cpu.affinity[[i]]))
  sched.info$job.pid <- sapply(X = sched.info$jobs,FUN = function(j) j$pid)
  
  # wait vars
  start.time = proc.time()[3]
  next.change = start.time + min(wait.at)
  sched.info$waiting = numeric(length(sched.info$job.id))
  sched.info$wtime = numeric(length(sched.info$job.id))
  sched.info$waiting.jobs = vector("list", length(sched.info$job.id))
  while (!all(fin)) {
    # fertige Jobs beenden
    #tmp = mccollect(sched.info$jobs[!is.na(sched.info$jobs)], wait = FALSE, timeout = 1)
    tmp = mccollect(wait = FALSE, timeout = 1)
    while (!is.null(tmp) && length(tmp) > 0){
      if(is.null(tmp[[1]])){
        tmp = tmp[-1]
        next()
      }
      coreid = which(sched.info$job.pid == names(tmp)[[1]])
      if (length(coreid) == 1){
        jobid = sched.info$job.id[coreid]
        write(paste0("end Job ", jobid, " pid: ", names(tmp)[[1]]), file = "scheduling", append = TRUE)
        res[[jobid]] = tmp[[1]]
        fin[jobid] = TRUE
        sched.info$job.pid[coreid] = NA
        sched.info$jobs[coreid] = NA
        sched.info$job.id[coreid] = NA
        tmp = tmp[-1]
        sched.info = startJob(coreid, sched.info, X, FUN)
      }else{
        warning("finished job in wait list, should be fine")
        wid = which(sapply(X = sched.info$waiting.jobs, FUN = function (j){ 
          if (!is.null(j)){
            return(j$pid == names(tmp)[[1]])
          } else {
            return(FALSE)
          }
        }))
        write(paste0("wlistend Job ", sched.info$waiting[wid]), file = "scheduling", append = TRUE)
        sched.info$ava[sched.info$waiting[wid],] = FALSE
        fin[sched.info$waiting[wid]] = TRUE
        sched.info$wtime[wid] = 0
        res[[sched.info$waiting[wid]]] = tmp[[1]]
        tmp = tmp[-1]
        system(paste0("kill -KILL ", sched.info$waiting.jobs[[wid]]$pid))#, intern = TRUE) #any other way?
      }
    }
    # Jobs bei Bedarf anhalten
    while(proc.time()[3] >= next.change){
      jobid = which.min(wait.at)
      wait.at[jobid] = Inf
      next.change = start.time + min(wait.at)
      coreid = which(sched.info$job.id == jobid)
      if(length(coreid) == 1){
        jobpid = sched.info$job.pid[coreid]
        write(paste0("pause Job: ", jobid,  " pid: ", jobpid), file = "scheduling", append = TRUE)
        if(is.na(jobpid)){
          warning("job pid not found, This should not happen")
          a = 1
        }
        system(paste0("kill -STOP ", jobpid), intern = TRUE)
        sched.info$waiting[coreid] = jobid
        sched.info$waiting.jobs[[coreid]] = sched.info$jobs[[coreid]]
        sched.info$wtime[[coreid]] = proc.time()[3]
        sched.info$ava[jobid, scheduled.on[[jobid]][2]] = TRUE
        sched.info$job.pid[coreid] = NA
        sched.info$jobs[coreid] = NA
        sched.info$job.id[coreid] = NA
        sched.info = startJob(coreid, sched.info, X, FUN)
      }
    }
  }
  mccollect()# kill childs
  for (i in sched.info$waiting){
    if (i > 0){
      res[[i]]$time = res[[i]]$time - sched.info$wtime[i]
      res[[i]]$user.extras$exec.time.real = res[[i]]$user.extras$exec.time.real - sched.info$wtime[i]
      attr(res[[i]]$y,"exec.time") = attr(res[[i]]$y,"exec.time") - sched.info$wtime[i]
    }
  }
  write(paste0("end Execution"), file = "scheduling", append = TRUE)
  write(paste0(" "), file = "scheduling", append = TRUE)
  return(res)
}