get_catalog_nhis <-
	function( data_name = "nhis" , output_dir , ... ){

		catalog <- NULL
		
		for( this_name in c( "nhpi" , "NHIS" ) ){
			
			base_ftp_dir <- paste0( "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/" , this_name , "/" )
		
			# read the text of the microdata ftp into working memory
			# download the contents of the ftp directory for all microdata
			ftp.listing <- strsplit( RCurl::getURL( base_ftp_dir , ssl.verifypeer = FALSE ) , "<br>" )[[1]]

			# extract the text from all lines containing a this_year of microdata
			# figure out the names of those this_year directories
			ay <- rev( gsub( '(.*)\\">(.*)<\\/A>$', "\\2" , ftp.listing ) )

			# remove non-numeric strings
			suppressWarnings( available_years <- ay[ as.numeric( ay ) %in% ay ] )
			# now `available.years` should contain all of the available years on the nhis ftp site

			for( this_year in available_years ){
			
				# define path of this year
				year_dir <- paste0( base_ftp_dir , this_year , "/" )

				cat( paste0( "loading " , data_name , " catalog from " , year_dir , "\r\n\n" ) )

				# just like above, read those lines into working memory
				this_listing <- strsplit( RCurl::getURL( year_dir , ssl.verifypeer = FALSE ) , "<br>" )[[1]]
				
				ftp_files <- tolower( gsub( '(.*)\\">(.*)<\\/A>$' , "\\2" , this_listing ) )
				
				# identify all .exe files..
				exe.filenames <- ftp_files[ grepl( ".exe" , ftp_files ) ]
				
				# identify all .zip files..
				zip.filenames <- ftp_files[ grepl( ".zip" , ftp_files ) ]
				
				# identify overlap between .zip and .exe files
				exe.filenames <- gsub( ".exe" , "" , exe.filenames )
				zip.filenames <- gsub( ".zip" , "" , zip.filenames )
				duplicate.filenames <- zip.filenames[ (zip.filenames %in% exe.filenames) ]
				zip.filenames.with.exe.matches <- paste( duplicate.filenames , ".zip" , sep = "" )
				
				# throw out .zip files that match a .exe file exactly
				ftp_files <- ftp_files[ ! ( ftp_files %in% zip.filenames.with.exe.matches ) ]
				
				# end of throwing out file.zip files that match file.exe files #	
				
				if( this_year == 2004 ){
				
					ftp_files <- gsub( "familyfile" , "familyfile/familyxx.exe" , ftp_files )
					ftp_files <- gsub( "household" , "household/househld.exe", ftp_files )
					ftp_files <- gsub( "injurypoison" , "injurypoison/injpoiep.exe" , ftp_files )
					ftp_files <- gsub( "person" , "person/personsx.exe" , ftp_files )
					ftp_files <- gsub( "sampleadult" , "sampleadult/samadult.exe" , ftp_files )
					ftp_files <- gsub( "samplechild" , "samplechild/samchild.exe" , ftp_files )
					ftp_files <- ftp_files[ !( ftp_files %in% c( "" , "injuryverbatim" ) ) ]
				
					
				}

				# throw out folders (assumed to be files without a . in them)
				# (any files in folders within the main this_year folder need to be downloaded separately)
				ftp_files <- ftp_files[ grepl( "\\." , ftp_files ) ]

						
				# skip txt files
				ftp_files <- ftp_files[ !grepl( '\\.txt' , ftp_files ) ]
				
				# skip the new sc_bwt files entirely
				ftp_files <- ftp_files[ !grepl( 'sc_bwt' , ftp_files ) ]
				
				# skip these 1963 files with irregular SAS importation scripts
				if ( this_year == 1963 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( 'condition.exe' , 'family.exe' , 'hospital.exe' , 'health_exp.exe' ) ) ]
				
				# skip these 1964 files with irregular SAS importation scripts
				if ( this_year == 1964 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( 'family.exe' , 'hospital.exe' , 'xray.exe' , 'person.exe' ) ) ]

				# skip these 1965 files with irregular SAS importation scripts
				if ( this_year == 1965 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "condition.exe" , "diabetes.exe" , "person.exe" , "presmed.exe" ) ) ]

				# skip these 1966 files with irregular SAS importation scripts
				if ( this_year == 1966 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "condition.exe" , "person.exe" ) ) ]

				# skip these 1969 files with irregular SAS importation scripts
				if ( this_year == 1969 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "aidsspec.exe" , "arthrtis.exe" ) ) ]

				# skip these 1970 files with irregular SAS importation scripts
				if ( this_year == 1970 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "healthin.exe" , "medccost.exe" , "xrayxxxx.exe" ) ) ]

				# the healthin file has WTBDD2W and WTBDD2WB (in the SAS input file) in the wrong order
				if ( this_year %in% c( 1972 , 1974 ) ) ftp_files <- ftp_files[ ! ( ftp_files %in% 'healthin.exe' ) ]
				
				# skip this 1973 file with irregular SAS importation scripts
				if ( this_year == 1973 ) ftp_files <- ftp_files[ ! ( ftp_files %in% "prgnancy.exe" ) ]
				
				# skip these 1977 files with irregular SAS importation scripts
				if ( this_year == 1977 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "aidsspec.exe" , "influenza.exe" ) ) ]
				
				# skip these 1978 files with irregular SAS importation scripts
				if ( this_year == 1978 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "famedexp.exe" , "immunize.exe" ) ) ]
				
				# skip 1979 personsx file
				if ( this_year == 1979 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "personsx.exe" , "smokingx.exe" ) ) ]

				# skip 1988 mdevices file
				if ( this_year == 1988 ) ftp_files <- ftp_files[ ! ( ftp_files %in% "mdevices.exe" ) ]

				# skip 1994 and 1995 dfs files
				if ( this_year %in% c( 1994 , 1995 ) ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "dfschild.exe" , "dfsadult.exe" ) ) ]

				# skip the 1992 nursing home files
				if ( this_year == 1992 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "conditnh.exe" , "drvisinh.exe" , "hospitnh.exe" , "househnh.exe" , "personnh.exe" ) ) ]

				# skip the 2007 alternative medicine and injury verbatim files
				if ( this_year == 2007 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "althealt.exe" , "injverbt.exe" ) ) ]

				# skip the 1999 and 2000 injury verbatim file
				if ( this_year %in% c( 1998:2000 , 2008 , 2009 ) ) ftp_files <- ftp_files[ ! ( ftp_files %in% "injverbt.exe" ) ]

				# skip the 2010 sad_wgts.dat and sc_bwt10.dat files (although you may need them, depending what you're doing!)
				if ( this_year == 2010 ) ftp_files <- ftp_files[ ! ( ftp_files %in% c( "sad_wgts.dat" , "sc_bwt10.dat" ) ) ]
				
				# skip csv files
				ftp_files <- ftp_files[ !grepl( "csv\\.zip$" , ftp_files , ignore.case = TRUE ) ]
				
				catalog <-
					rbind(
						catalog ,
						data.frame(
							year = this_year ,
							type = tolower( gsub( "\\.(.*)" , "" , basename( ftp_files ) ) ) ,
							full_url = paste0( year_dir , ftp_files ) ,
							nhpi = ( this_name == "nhpi" ) ,
							stringsAsFactors = FALSE
						)
					)
				
			}
			
		}

		catalog$output_filename <-
			paste0( output_dir , "/" , catalog$year , "/" , ifelse( catalog$nhpi , "nhpi/" , "" ) , gsub( "\\.(.*)" , ".rds" , basename( catalog$full_url ) ) )
			
		catalog$sas_script <- 
			paste0( gsub( "Datasets" , "Program_Code" , dirname( catalog$full_url ) ) , "/" , gsub( "\\.rds" , ".sas" , basename( catalog$output_filename ) ) )
		
		catalog$sas_script <- gsub( "nhpi/" , "NHPI/" , catalog$sas_script )
		
		catalog$output_filename <- gsub( "nhpi/" , "nhpi_" , catalog$output_filename )

		catalog$imputed_income <- FALSE
		
		available_imputed_incomes <- grep( "imputed_income" , ay , value = TRUE , ignore.case = TRUE )

		for( this_income in available_imputed_incomes ){
		
			# define path of this imputed income file
			income_dir <- paste0( base_ftp_dir , this_income , "/" )

			cat( paste0( "loading " , data_name , " catalog from " , income_dir , "\r\n\n" ) )

			this_listing <- strsplit( RCurl::getURL( income_dir , ssl.verifypeer = FALSE ) , "<br>" )[[1]]
			
			ftp_files <- tolower( gsub( '(.*)\\">(.*)<\\/A>$' , "\\2" , this_listing ) )
	
			# remove stata files and missing files
			ftp_files <- ftp_files[ !( ftp_files %in% "" ) & !grepl( "\\.do$" , ftp_files ) ]

			# base the catalog off of the sas scripts
			inc_cat <-
				data.frame(
					year = gsub( "_imputed_income" , "" , this_income , ignore.case = TRUE ) ,
					type = "ii" ,
					sas_script = paste0( income_dir , grep( "\\.sas$" , ftp_files , value = TRUE , ignore.case = TRUE ) ) ,
					imputed_income = TRUE ,
					nhpi = FALSE ,
					stringsAsFactors = FALSE
				)
				
			for( j in seq_len( nrow( inc_cat ) ) ){
				inc_cat[ j , 'full_url' ] <-
					paste0( 
						income_dir , 
						ftp_files[ ftp_files %in% paste0( gsub( "\\.sas" , "" , basename( inc_cat[ j , 'sas_script' ] ) , ignore.case = TRUE ) , c( '.zip' , '.exe' ) ) ]
					)
			}
					
			inc_cat$output_filename <-
				paste0( output_dir , "/" , inc_cat$year , "/" , gsub( "\\.(.*)" , ".rds" , basename( inc_cat$full_url ) ) )
			
			catalog <- rbind( catalog , inc_cat )
		}
		
		catalog <- catalog[ order( catalog[ , 'year' ] ) , ]
		
		catalog

	}


lodown_nhis <-
	function( data_name = "nhis" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()
	
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			if( catalog[ i , 'imputed_income' ] ){
			
				SAScii_start <- grep( "INPUT ALL VARIABLES" , readLines( catalog[ i , 'sas_script' ] , encoding = 'latin1' ) ) + 1
				
				# unzip the file into a temporary directory.
				# the unzipped file should contain *five* ascii files
				income_file_names <- sort( unzipped_files )
					
				# loop through all five imputed income files
				for ( j in 1:length( income_file_names ) ){

					ii <- read_SAScii( income_file_names[ j ] , catalog[ i , 'sas_script' ] , beginline = SAScii_start , sas_encoding = 'latin1' )

					names( ii ) <- tolower( names( ii ) )

					ii$rectype <- NULL
					
					assign( paste0( "ii" , j ) , ii )
					
					
				}
				
				catalog[ i , 'case_count' ] <- nrow( ii )
				
				# save all five imputed income data frames to a single .rds file #
				saveRDS( mget( paste0( "ii" , 1:5 ) ) , file = catalog[ i , 'output_filename' ] , compress = FALSE )
						
			
			} else {
				
				# if the zipped file includes a csv file, pick only the `.dat` file instead
				if( length( unzipped_files ) > 1 ) unzipped_files <- unzipped_files[ grep( "\\.dat$" , tolower( unzipped_files ) ) ]
				
				# ..and read that text file directly into an R data.frame
				# using the sas importation script downloaded before this big fat loop
				x <- read_SAScii( unzipped_files , catalog[ i , "sas_script" ] , sas_encoding = 'latin1' )
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )
				
				catalog[ i , 'case_count' ] <- nrow( x )
				
				saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			}
			
			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

