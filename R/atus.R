#' @importFrom utils download.file read.csv unzip getFromNamespace

get_catalog_atus <-
  function( data_name = "atus" , ... ){

    catalog <- NULL

    tf <- tempfile()

    years.to.download <-
      # figure out which years are available to download
      unique(
        gsub(
          "(.*)/datafiles_([0-9][0-9][0-9][0-9]).htm(.*)" ,
          "\\2" ,
          grep(
            "/datafiles_([0-9][0-9][0-9][0-9]).htm" ,
            readLines( "https://www.bls.gov/tus/" , warn = FALSE ) ,
            value = TRUE
          )
        )
      )

    for( year in years.to.download ){

      # figure out the website listing all available zipped files
      http.page <-
        paste0(
          "https://www.bls.gov/tus/datafiles_" ,
          year ,
          ".htm"
        )


      cat( paste0( "loading " , data_name , " catalog from" , http.page , "\r\n\n" ) )

      # download the contents of the website
      # to the temporary file
      download.file( http.page , tf , mode = 'wb' , quiet = TRUE )

      # read the contents of that temporary file
      # into working memory (a character object called `txt`)
      txt <- readLines( tf , warn = FALSE )
      # if the object `txt` contains the page's contents,
      # you're cool.  otherwise, maybe look at this discussion
      # http://stackoverflow.com/questions/5227444/recursively-ftp-download-then-extract-gz-files
      # ..and tell me what you find.

      # keep only lines with a link to data files
      txt <- txt[ grep( ".zip" , txt , fixed = TRUE ) ]

      # isolate the zip filename #

      # first, remove everything before the `special.requests/tus/`..
      txt <- sapply( strsplit( txt , "/tus/special.requests/" ) , "[[" , 2 )

      # ..second, remove everything after the `.zip`
      all.files.on.page <- sapply( strsplit( txt , '.zip\">' ) , "[[" , 1 )

      # now you've got all the basenames
      # in the object `all.files.on.page`

      # remove all `lexicon` files.
      # you can download a specific year
      # for yourself if ya want.
      all.files.on.page <-
        all.files.on.page[ !grepl( 'lexiconwex' , all.files.on.page ) ]

      catalog <- rbind( catalog , data.frame( directory = year , rda = all.files.on.page ) )

    }

    catalog
  }


lodown_atus <-
  function( catalog , data_name = "atus" , ... ){

    tf <- tempfile()

    http.dir <- "https://www.bls.gov/tus/special.requests/"

    for ( i in seq_len( nrow( catalog ) ) ){

      # build a character string containing the
      # full filepath to the current zipped file
      fn <- paste0( http.dir , catalog[ i , 'rda' ] , ".zip" )

      # download the file
      cache_download( fn , tf , mode = 'wb' )

      # extract the contents of the zipped file
      # into the current year-specific directory
      # and (at the same time) create an object called
      # `files.in.zip` that contains the paths on
      # your local computer to each of the unzipped files
      files.in.zip <-
        unzip( tf , exdir = paste0( "./" , catalog[ i , 'directory' ] ) )

      # find the data file
      csv.file <-
        files.in.zip[ grep( ".dat" , files.in.zip , fixed = TRUE ) ]

      # read the data file in as a csv
      x <- read.csv( csv.file , stringsAsFactors = FALSE )

      # convert all column names to lowercase
      names( x ) <- tolower( names( x ) )

      # remove the _YYYY from the string containing the filename
      savename <- gsub( paste0( "_" , catalog[ i , 'directory' ] ) , "" , catalog[ i , 'rda' ] )

      # copy the object `x` over to another object
      # called whatever's in savename
      assign( savename , x )

      # delete the object `x` from working memory
      rm( x )

      # save the object named within savename
      # into an R data file (.rda) for easy loading later
      save(
        list = savename ,
        file = paste0( "./" , catalog[ i , 'directory' ] , "/" , catalog[ i , 'rda' ] , ".rda" )
      )

      # delete the savename object from working memory
      rm( list = savename )

      # clear up RAM
      gc()

      # delete the files that were unzipped
      # at the start of this loop,
      # including any directories
      unlink( files.in.zip , recursive = TRUE )

      # delete the temporary file
      # (which stored the zipped file)
      file.remove( tf )


      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , getwd() , "/" , catalog[ i , 'directory' ] , "/" , catalog[ i , 'rda' ] , ".rda'\r\n\n" ) )

  }

  cat( paste0( data_name , " download completed\r\n\n" ) )

  invisible( TRUE )

}
