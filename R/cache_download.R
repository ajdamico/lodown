

# from http://stackoverflow.com/questions/16474696/read-system-tmp-dir-in-r
gettmpdir <-
  function() {
    tm <- Sys.getenv(c('TMPDIR', 'TMP', 'TEMP'))
    d <- which(file.info(tm)$isdir & file.access(tm, 2) == 0)
    if (length(d) > 0)
      tm[[d[1]]]
    else if (.Platform$OS.type == 'windows')
      Sys.getenv('R_USER')
    else '/tmp'
  }


rcurl_filesize <-
    function( url ){
      xx <- RCurl::getURL(url, nobody=1L, header=1L)
      yy <- strsplit(xx, "\r\n")[[1]]
      as.numeric( gsub( "Content-Length: " , "" , grep( "Content-Length" , yy , value = TRUE ) ) )
    }

httr_filesize <-
    function( url ){
      xx <- httr::HEAD(url)
      yy <- httr::headers(xx)$`content-length`
      as.numeric( yy )
    }


cache_download <-
  function (

    url ,

    destfile = NULL ,

    # pass in any other arguments needed for the FUN
    ... ,

    # specify which download function to use.
    # `download.file` and `downloader::download` should both work.
    FUN = download.file ,

    # how many attempts should be made with FUN?
    attempts = 3 ,
    # just in case of a server timeout or smthn equally annoying

    # how long should cache_download wait between attempts?
    sleepsec = 60 ,
	
	# which filesize function should be used
	# c( 'rcurl' , 'httr' )
	filesize_fun = 'rcurl'
	
  ) {

    if( filesize_fun == 'rcurl' ) this_filesize <- rcurl_filesize( url )
	
	if( filesize_fun == 'httr' ) this_filesize <- httr_filesize( url )
	

    if( this_filesize == 0 ) stop( "remote server lists file size as zero" )

    urlhash <- digest::digest(url)

    cachefile <-
      paste0(
        gsub( "\\" , "/" , gettmpdir() , fixed = TRUE ) ,
        "/" ,
        urlhash ,
        ".Rdownloadercache"
      )

    if( file.exists( cachefile ) ){

      if( is.null( destfile ) ){

          load( cachefile )

          if( length( success ) == this_filesize | length( httr::content( success ) ) == this_filesize ){

            cat( paste0( "'" , url , "' cached in '" , cachefile , "', returning object\r\n\n" ) )

            return( invisible( success ) )

          } else rm( success )

      } else {

         if ( file.info( cachefile )$size == this_filesize ){

          cat( paste0( "'" , url , "' cached in '" , cachefile , "', copying to '" , destfile , "'\r\n\n" ) )

          return( invisible( ifelse( file.copy( cachefile , destfile , overwrite = TRUE ) , 0 , 1 ) ) )

        }

      }
    }


    if( is.null( destfile ) ){
      cat(
        paste0(
          "saving from URL '" ,
          url ,
          "' to this object... \r\n\n"
        )
      )
    } else {
      cat(
        paste0(
          "Downloading from URL '" ,
          url ,
          "' to file '" ,
          destfile ,
          "'... \r\n\n"
        )
      )
    }

    # start out with a failed attempt, so the while loop below commences
    failed.attempt <- try( stop() , silent = TRUE )

    # keep trying the download until you run out of attempts
    # and all previous attempts have failed

    initial.attempts <- attempts

    while( attempts > 0 & class( failed.attempt ) == 'try-error' ){

      # only run this loop a few times..
      attempts <- attempts - 1

      failed.attempt <-
        try( {

          # if there is no destination file, then `success` contains the data.
          if( is.null( destfile ) ){

            # did the download work?
            success <-
              do.call(
                FUN ,
                list( url , ... )
              )

            if( length( success ) != this_filesize && length( httr::content( success ) ) != this_filesize ){

              message( paste0( "downloaded binary url size (" , length( success ) , ") does not match server's content length (" , this_filesize , ")" ) )

              class(failed.attempt) <- 'try-error'

              rm( success )

            }

          } else {

            # did the download work?
            success <-
              do.call(
                FUN ,
                list( url , destfile , ... )
              ) == 0

            if( file.info( destfile )$size != this_filesize ){

                message( paste0( "downloaded file size on disk (" , file.info( destfile )$size , ") does not match server's content length (" , this_filesize , ")" ) )

                class(failed.attempt) <- 'try-error'

                rm( success )
            }

          }

        } ,
        silent = TRUE
        )

      # if the download did not work, wait `sleepsec` seconds and try again.
      if( class( failed.attempt ) == 'try-error' ){
        cat( paste0( "download issue with '" , url , "'\r\n\n" ) )
        Sys.sleep( sleepsec )
      }

    }

    # double-check that the `success` object exists.. it might not if `attempts` was set to zero.
    if ( exists( 'success' ) ){

      if( is.null( destfile ) ){

        save( success , file = cachefile )

      } else file.copy( destfile , cachefile , overwrite = TRUE )

      return( invisible( success ) )

      # otherwise break.
    } else stop( paste( "download failed after" , initial.attempts , "attempts" ) )

  }
