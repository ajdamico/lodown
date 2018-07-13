get_catalog_nis <-
  function( data_name = "nis" , output_dir , ... ){

	combined_paths <- NULL

	for( nis_ftp_site in
		c(
			"https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/nis/" ,
			"https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/nis/NHFS/"
		) ) {

		nis_ftp_contents <- RCurl::getURL( nis_ftp_site , dirlistonly = TRUE , ssl.verifypeer = FALSE )

		nis_ftp_paths <- paste0( nis_ftp_site , strsplit( nis_ftp_contents , '(\r)?\n' )[[1]] )

		combined_paths <- c( combined_paths , nis_ftp_paths )

	}

	for( nis_ftp_site in
		c(
			"https://ftp.cdc.gov/pub/Vaccines_NIS/" ,
			"https://ftp.cdc.gov/pub/vaccines2/nis-teen/"
		) ) {

		nis_ftp_contents <- RCurl::getURL( nis_ftp_site , dirlistonly = TRUE , ssl.verifypeer = FALSE )

		possible_filenames <- grep( "\\.dat$|\\.R$" , strsplit( nis_ftp_contents , '>|<' )[[1]] , value = TRUE , ignore.case = TRUE )

		nis_ftp_paths <- paste0( nis_ftp_site , possible_filenames )

		combined_paths <- c( combined_paths , nis_ftp_paths )

	}

	dat_files <- grep( "\\.dat$|dat\\.zip$" , combined_paths , value = TRUE , ignore.case = TRUE )

	dat_years <- gsub( "(.*)([0-9][0-9])(.*)" , "\\2" , basename( dat_files ) )

	dat_years[ dat_years == "NHFSPUF.DAT" ] <- "09"

	dat_years <- ifelse( as.numeric( dat_years ) > 94 , 1900 + as.numeric( dat_years ) , 2000 + as.numeric( dat_years ) )

	catalog <-
		data.frame(
			full_url = dat_files ,
			year = dat_years ,
			directory = ifelse( grepl( "NHFS" , dat_files ) , "flu" , ifelse( grepl( "TEEN" , toupper( dat_files ) ) , "teen" , "main" ) ) ,
			stringsAsFactors = FALSE
		)

	# zipped files first
	catalog <- catalog[ order( catalog$year , catalog$directory , !grepl( "zip" , catalog$full_url , ignore.case = TRUE ) ) , ]

	# remove duplicates
	catalog <- catalog[ !duplicated( catalog[ , c( 'year' , 'directory' ) ] ) , ]

	# determine related R scripts
	r_scripts <- as.character( sapply( paste0( ifelse( catalog$directory == 'flu' , 'nhfs' , ifelse( catalog$directory == 'teen' , 'teen' , 'nis' ) ) , "puf" , ifelse( catalog$directory != 'flu' , substr( catalog$year , 3 , 4 ), "" ) , "\\.r" ) , grep , combined_paths , ignore.case = TRUE , value = TRUE ) )

	catalog$r_script <-
		ifelse( catalog$year == 2015 ,
			paste0( "https://www.cdc.gov/vaccines/imz-managers/nis/downloads/nis-" ,
				ifelse( catalog$directory == 'teen' , "teen-" , "" ) ,
				"puf" ,
				substr( catalog$year , 3 , 4 ) ,
				".r" ) ,
		ifelse( r_scripts == 'character(0)' , NA ,
			r_scripts ) )

	# determine related sas scripts
	sas_scripts <- as.character( sapply( paste0( ifelse( catalog$directory == 'flu' , 'nhfs' , ifelse( catalog$directory == 'teen' , 'teen' , 'nis' ) ) , "puf" , ifelse( catalog$directory != 'flu' , substr( catalog$year , 3 , 4 ), "" ) , "\\.sas" ) , grep , combined_paths , ignore.case = TRUE , value = TRUE ) )

	catalog$sas_script <-
		ifelse( catalog$year > 2014 , gsub( "\\.r$" , "\\.sas" , catalog$r_script ) ,
		ifelse( sas_scripts == 'character(0)' , NA , sas_scripts ) )

	catalog$output_filename <- paste0( output_dir , "/" , catalog$year , " " , catalog$directory , ".rds" )

	catalog

  }


lodown_nis <-
  function( data_name = "nis" , catalog , ... ){

	on.exit( print( catalog ) )

	tf <- tempfile()

	for ( i in seq_len( nrow( catalog ) ) ){

		# download the file
		cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

		if( grepl( "\\.zip$" , catalog[ i , "full_url" ] , ignore.case = TRUE ) ){

			unzipped_file <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) )

			if( length( unzipped_file ) > 1 ) stop( "only expecting one file" )

			file.copy( unzipped_file , tf , overwrite = TRUE )

			file.remove( unzipped_file )

		} else unzipped_file <- basename( catalog[ i , "full_url" ] )

		file.copy( tf , paste0( tempdir() , "/" , basename( unzipped_file ) ) )

		if( !is.na( catalog[ i , 'r_script' ] ) ){

			before_encoding <- getOption( "encoding" )

			options( encoding = "windows-1252" )

			# load the r script into a character vector
			httr::GET( catalog[ i , 'r_script' ] , httr::write_disk( tf , overwrite = TRUE ) )
			script.r <- readLines ( tf )

			# remove the Hmisc library
			script.r <- script.r[ !grepl( 'library(Hmisc)' , script.r , fixed = TRUE ) ]
			
			# change the path to the data to the local working directory
			script.r <- gsub( "path-to-data" , normalizePath( tempdir() , winslash = "/" ) , script.r , fixed = TRUE )

			# change the path to the file to the public use file directory within your current working directory
			script.r <- gsub( "path-to-file" , normalizePath( tempdir() , winslash = "/" ) , script.r , fixed = TRUE )

			# correct lines of the r script that just are not allowed
			script.r <- gsub( "IHQSTATUSlevels=c(,M,N,V)" , "IHQSTATUSlevels=c(NA,'M','N','V')" , script.r , fixed = TRUE )

			# this line also creates an error.  nope.  fix it.
			script.r <- gsub( "=c(," , "=c(NA," , script.r , fixed = TRUE )

			# everything after `Step 4:   ASSIGN VARIABLE LABELS` is unnecessary
			# converting these variables to factors blanks out many values that should not be blanked out
			# for a prime example, see what happens to the `seqnumhh` column.  whoops.

			# figure out the line position of step four within the character vector
			cutoff <- max( grep( "ASSIGN VARIABLE LABELS" , script.r , fixed = TRUE ) )

			# reduce the r script to its contents from the beginning up till step four
			script.r <- script.r[ seq( cutoff ) ]

			# save the r script back to the local disk
			writeLines( script.r , tf )

			# run the now-reduced r script
			source( tf )

			options( encoding = before_encoding )

			# create a character string containing the name of the nis puf data.frame object
			nis.df <-
				paste0(
					'NIS' ,
					if( catalog[ i , 'directory' ] == 'teen' ) 'TEEN' ,
					'PUF' ,
					substr( catalog[ i , 'year' ] , 3 , 4 )
				)

			# copy the data.frame produced by the r script over to the object `x`
			if( catalog[ i , 'directory' ] == 'flu' ) x <- get( "NHFSPUF" ) else x <- get( nis.df )

			rm( list = nis.df )

		} else {

			script.txt <- readLines( catalog[ i , 'sas_script' ] , warn = FALSE )

			# throw out everything at and after section d
			script.sub <- script.txt[ grep( "D. CREATES PERMANENT SAS DATASET|INFILE &flatfile LRECL=721|INFILE &flatfile LRECL=773" , script.txt ):length( script.txt ) ]

			# save the reduced sas import script to the local disk
			writeLines( script.sub , tf )

			x <- read_SAScii( paste0( tempdir() , "/" , basename( unzipped_file ) ) , tf )

		}

      # convert all column names to lowercase
      names( x ) <- tolower( names( x ) )

	  # add a column of ones
	  x$one <- 1

	  catalog[ i , 'case_count' ] <- nrow( x )

      saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

      # delete the temporary files
      file.remove( tf , paste0( tempdir() , "/" , basename( unzipped_file ) ) )

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

    }

	on.exit()
	
    catalog

  }

