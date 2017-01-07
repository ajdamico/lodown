get_catalog_ahrf <-
  function( data_name = "ahrf" , output_dir , ... ){

    lines_with_links <- grep( "https://(.*)\\.zip" , readLines( "https://ahrf.hrsa.gov/download.htm" , warn = FALSE ) , value = TRUE , ignore.case= TRUE )

    lines_with_links <- lines_with_links[ !grepl( "MS Access" , lines_with_links ) ]

    full_url <- gsub( '(.*)href=\"(.*)\"(.*)' , '\\2' , lines_with_links )

    this_catalog <-
      data.frame(
          directory = ifelse( grepl( "_SN_" , full_url ) , "state" , "county" ) ,
          tech_doc = grepl( "_USER_TECH_" , full_url ) ,
          year = gsub( "(.*)([0-9][0-9][0-9][0-9])-([0-9][0-9][0-9][0-9])(.*)" , "\\2" , full_url )
      )

    this_catalog$full_url = as.character( full_url )

	this_catalog$output_filename <- paste0( output_dir , "/" , this_catalog$directory , "/" , gsub( "\\.zip" , ".rda" , basename( this_catalog$full_url ) ) )
	
    this_catalog[ !this_catalog[ , "tech_doc" ] , ]
  }


lodown_ahrf <-
  function( data_name = "ahrf" , catalog , ... ){

    tf <- tempfile()


    for ( i in seq_len( nrow( catalog ) ) ){

      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )


      # extract the contents of the zipped file
      # into the current year-specific directory
      # and (at the same time) create an object called
      # `unzipped_files` that contains the paths on
      # your local computer to each of the unzipped files
      unzipped_files <- unzip_warn_fail( tf , exdir = dirname( catalog[ i , 'output_filename' ] ) )

      sas_path <- grep( "\\.sas$" , unzipped_files , value = TRUE )

      dat_path <- grep( "\\.asc$" , unzipped_files , value = TRUE )

	  x <- read_SAScii( dat_path , sas_path , na_values = "." )

      # convert all column names to lowercase
      names( x ) <- tolower( names( x ) )

      save( x , file = catalog[ i , 'output_filename' ] )

	  # delete the temporary files
      file.remove( tf )

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

    }

    invisible( TRUE )

  }

