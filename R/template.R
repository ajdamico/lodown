# get_catalog_template <-
  # function( data_name = "template" , ... ){

	# catalog <-
	  # data.frame(
		  # directory = ifelse( grepl( "_SN_" , path_to_files ) , "state" , "county" ) ,
		  # tech_doc = grepl( "_USER_TECH_" , path_to_files ) ,
		  # year = gsub( "(.*)([0-9][0-9][0-9][0-9])-([0-9][0-9][0-9][0-9])(.*)" , "\\2" , basename( path_to_files ) ) ,
		  # full_url = path_to_files ,
		  # stringsAsFactors = FALSE
	  # )
	  
	# catalog
  
  # }


# lodown_template <-
  # function( catalog , data_name = "template" , ... ){

    # if (!requireNamespace("template", quietly = TRUE)) stop("template needed for this function to work. Please install it.", call. = FALSE)

    # tf <- tempfile()


    # for ( i in seq_len( nrow( catalog ) ) ){

      # # download the file
      # cache_download( catalog[ i , "urls" ] , tf , mode = 'wb' )

      # unzipped_files <- unzip( tf , exdir = paste0( "./" , catalog[ i , 'directory' ] ) )

	  
	  
	  
	  
	  
	  
      # # convert all column names to lowercase
      # names( x ) <- tolower( names( x ) )

      # save( x , file = paste0( "./" , catalog[ i , 'geography' ] , "/" , gsub( "\\.zip" , ".rda" , basename( catalog[ i , 'urls' ] ) ) ) )

      # # delete the temporary file
      # file.remove( tf )


      # cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , getwd() , "/" , catalog[ i , 'directory' ] , "/" , gsub( "\\.zip" , ".rda" , basename( catalog[ i , 'urls' ] ) ) , "'\r\n\n" ) )

    # }

    # cat( paste0( data_name , " download completed\r\n\n" ) )

    # invisible( TRUE )

  # }

