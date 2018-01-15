get_catalog_atus <-
	function( data_name = "atus" , output_dir , ... ){

		catalog <- NULL

		tf <- tempfile()

		years.to.download <-
		# figure out which years are available to download
		unique( gsub( "(.*)/datafiles_([0-9][0-9][0-9][0-9]).htm(.*)" , "\\2" , grep( "/datafiles_([0-9][0-9][0-9][0-9]).htm" , readLines( "https://www.bls.gov/tus/" , warn = FALSE ) , value = TRUE ) ) )

		for( year in years.to.download ){

			# figure out the website listing all available zipped files
			http.page <- paste0( "https://www.bls.gov/tus/datafiles_" , year , ".htm" )

			cat( paste0( "loading " , data_name , " catalog from " , http.page , "\r\n\n" ) )

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
			files_on_page <- sapply( strsplit( txt , '.zip\">' ) , "[[" , 1 )

			# now you've got all the basenames
			# in the object `files_on_page`

			# remove all `lexicon` files.
			# you can download a specific year
			# for yourself if ya want.
			files_on_page <- files_on_page[ !grepl( 'lexiconwex' , files_on_page ) ]

			catalog <- 
				rbind( 
					catalog , 
					data.frame( 
						directory = year , 
						rds = files_on_page , 
						full_url = paste0( "https://www.bls.gov/tus/special.requests/" , files_on_page , ".zip" ) ,
						output_filename = paste0( output_dir , "/" , year , "/" , gsub( "_([0-9][0-9][0-9][0-9])" , "" , files_on_page ) , ".rds" ) ,
						stringsAsFactors = FALSE
					)
				)

		}

		catalog
	}


lodown_atus <-
	function( data_name = "atus" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )

			# extract the contents of the zipped file
			# into the current year-specific directory
			# and (at the same time) create an object called
			# `unzipped_files` that contains the paths on
			# your local computer to each of the unzipped files
			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# find the data file
			csv_file <- unzipped_files[ grep( ".dat" , unzipped_files , fixed = TRUE ) ]

			# read the data file in as a csv
			x <- read.csv( csv_file , stringsAsFactors = FALSE )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			# add the number of records to the catalog
			catalog[ i , 'case_count' ] <- nrow( x )

			# save the object named within savename
			# into an R data file (.rds) for easy loading later
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			# delete the temporary files
			file.remove( unzipped_files , tf )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}
