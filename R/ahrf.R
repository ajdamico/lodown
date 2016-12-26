get_catalog_ahrf <-
  function( ... ){

    lines_with_links <- grep( "https://(.*)\\.zip" , readLines( "https://ahrf.hrsa.gov/download.htm" , warn = FALSE ) , value = TRUE , ignore.case= TRUE )

    lines_with_links <- lines_with_links[ !grepl( "MS Access" , lines_with_links ) ]

    urls <- gsub( '(.*)href=\"(.*)\"(.*)' , '\\2' , lines_with_links )

    this_catalog <-
      data.frame(
          geography = ifelse( grepl( "_SN_" , urls ) , "state" , "county" ) ,
          tech_doc = grepl( "_USER_TECH_" , urls ) ,
          year = gsub( "(.*)([0-9][0-9][0-9][0-9])-([0-9][0-9][0-9][0-9])(.*)" , "\\2" , urls )
      )

    this_catalog$urls = as.character( urls )

    this_catalog[ !this_catalog[ , "tech_doc" ] , ]
  }


lodown_ahrf <-
  function( catalog , data_name = "ahrf" , ... ){

    if (!requireNamespace("readr", quietly = TRUE)) stop("readr needed for this function to work. Please install it.", call. = FALSE)

    tf <- tempfile()


    for ( i in seq_len( nrow( catalog ) ) ){

      # download the file
      cache_download( catalog[ i , "urls" ] , tf , mode = 'wb' )


      # extract the contents of the zipped file
      # into the current year-specific directory
      # and (at the same time) create an object called
      # `files.in.zip` that contains the paths on
      # your local computer to each of the unzipped files
      files.in.zip <- unzip( tf , exdir = paste0( "./" , catalog[ i , 'geography' ] ) )

      sas_path <- grep( "\\.sas$" , files.in.zip , value = TRUE )

      dat_path <- grep( "\\.asc$" , files.in.zip , value = TRUE )

      sasc <- SAScii::parse.SAScii( sas_path )

      sasc$varname[ is.na( sasc$varname ) ] <- paste0( "toss" , seq( sum( is.na( sasc$varname ) ) ) )

      # read in the fixed-width file..
      x <-
        readr::read_fwf(
          # using the ftp filepath
          dat_path ,
          # using the parsed sas widths
          readr::fwf_widths( abs( sasc$width ) , col_names = sasc[ , 'varname' ] ) ,
          # using the parsed sas column types
          col_types = paste0( ifelse( grepl( "^toss" , sasc$varname ) , "_" , ifelse( sasc$char , "c" , "d" ) ) , collapse = "" ) ,

          na = "."
        )

      x <- data.frame( x )

      # convert all column names to lowercase
      names( x ) <- tolower( names( x ) )

      save( x , file = paste0( "./" , catalog[ i , 'geography' ] , "/" , gsub( "\\.zip" , ".rda" , basename( catalog[ i , 'urls' ] ) ) ) )

      # delete the temporary file
      # (which stored the zipped file)
      file.remove( tf )


      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , getwd() , "/" , catalog[ i , 'geography' ] , "/" , gsub( "\\.zip" , ".rda" , basename( catalog[ i , 'urls' ] ) ) , "'\r\n\n" ) )

    }

    cat( paste0( data_name , " download completed\r\n\n" ) )

    invisible( TRUE )

  }

