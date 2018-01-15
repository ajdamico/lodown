get_catalog_pls <-
	function( data_name = "pls" , output_dir , ... ){

		pls.page <- readLines( "https://www.imls.gov/research-evaluation/data-collection/public-libraries-survey/explore-pls-data/pls-data" , warn = FALSE )

		# restrict this page to only the records that contain `_csv` links
		pls.links <- pls.page[ grep( '_csv' , pls.page ) ]

		# re-name these zipped-file-links to only the pre-csv filepath
		pls.files <- gsub( "(.*)files/(.*)_csv\\.zip(.*)" , "\\2" , pls.links )

		years <- as.numeric( gsub( "(.*)([0-9][0-9])(.*)" , "\\2" , gsub( "_20" , "" , pls.files ) ) )
		
		years <- ifelse( years > 91 , years + 1900 , years + 2000 )
		
		catalog <-
			data.frame(
				year = years ,
				full_url = paste0( "https://www.imls.gov/sites/default/files/" , pls.files , "_csv.zip" ) ,
				output_folder = paste0( output_dir , "/" , years , "/" ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_pls <-
	function( data_name = "pls" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )
			
			# loop through each of the files extracted to the temporary directory..
			for ( this_csv in unzipped_files ){
			
				# figure out the name of the object to be saved,
				# by removing all text after the dot
				this.tablename <- gsub( "\\.(.*)" , "" , basename( this_csv ) )
			
				# remove all numeric characters, and also conver the entire string to lowercase
				this.tablename <- tolower( gsub( "[0-9]" , "" , this.tablename ) )
			
				# read the csv file into an R data.frame
				x <- read.csv( this_csv , stringsAsFactors = FALSE )
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )
			
				# figure out which columns are integer typed
				int.cols <- sapply( x , class ) == 'integer'
				
				# for all integer columns, replace negative ones with NAs
				x[ , int.cols ] <- sapply( x[ , int.cols ] , function( z ){ z[ z == -1 ] <- NA ; z } )
				
				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )
					
				# save this table to a year x tablename path in the current working directory
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , this.tablename , ".rds" ) , compress = FALSE )

			}

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

