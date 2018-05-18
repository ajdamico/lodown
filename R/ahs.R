get_catalog_ahs <-
	function( data_name = "ahs" , output_dir , ... ){

		catalog <- NULL

		# create a temporary file
		tf <- tempfile()
		
		# figure out available ahs years

		# hard-code the location of the census bureau's all ahs data page
		download.file( "https://www.census.gov/programs-surveys/ahs/data.All.html" , tf , mode = 'wb' )

		# split up the page into separate lines
		http.contents <- readLines( tf )

		# look for four-digit years in html filenames
		available_years <- 
			gsub( 
				"(.*)>(.*)<(.*)" , 
				"\\2" , 
				grep( "https://www.census.gov/programs-surveys/ahs/data.([0-9][0-9][0-9][0-9]).html" , http.contents , value = TRUE )
			)


		for( year in available_years ){

			puf.pages <- precise.files <- NULL
		
			download.file( paste0( "https://www.census.gov/programs-surveys/ahs/data." , year , ".html" ) , tf , mode = 'wb' )

			# isolate all puf lines
			puf.lines <- gsub('(.*)href=\"(.*)\" title(.*)' , '\\2' , grep( "href(.*)Public Use" , readLines( tf ) , value = TRUE ) )
				
			download.file( paste0( "https://www.census.gov/" , puf.lines ) , tf , mode = 'wb' )
			
			these_lines <- gsub('(.*)href=\"(.*)\" title(.*)' , '\\2' , grep( "href(.*)Public Use" , readLines( tf ) , value = TRUE ) )
			
			these_lines <- these_lines[ !grepl( "mailto" , these_lines ) ] 
			
			# extract only the link
			puf.pages <- unique( c( puf.lines , these_lines ) )

			# ..loop through each puf page searching for zipped files
			for ( this.page in puf.pages ){

				this.contents <- strsplit( httr::content( httr::GET( paste0( "https://www.census.gov/" , this.page ) ) , 'text' ) , '\n' )[[1]]

				zipped.file.lines <- this.contents[ grep( "\\.zip" , tolower( this.contents ) ) ]

				precise.files <- unique( c( precise.files , gsub( '\"(.*)' , "" , gsub( '(.*)href=\"' , "" , zipped.file.lines ) ) ) )
				
			}
				
			# trim whitespace
			precise.files <- stringr::str_trim( precise.files )

			# remove empty strings
			precise.files <- precise.files[ precise.files != '' ]

				


			# look for exact matches, only zipped.
			pfi <- gsub( '\\.zip|\\.Zip|\\.ZIP' , "" , precise.files )

			# these files match a `.zip` file
			zip.matches <- pfi[ duplicated( pfi ) ]

			# get rid of the unzipped version if there's a zipped version.
			precise.files <- precise.files[ !( precise.files %in% zip.matches ) ]

			# look for sas and csv exact matches.
			pfi <- gsub( 'CSV' , 'SAS' , precise.files )

			# these files match a `csv` file
			sas.matches <- pfi[ duplicated( tolower( pfi ) ) ]

			# get rid of the sas version if there's a csv version.
			precise.files <- precise.files[ !( tolower( precise.files ) %in% tolower( sas.matches ) ) ]

			# do not download flat files, since they're only in sas7bdat and so hard to import.
			precise.files <- precise.files[ !grepl( 'flat' , tolower( precise.files ) ) ]

			# remove the 2011 sas file, there's a similiar (though differently named) csv file
			precise.files <-
				precise.files[ precise.files != "https://www2.census.gov/programs-surveys/ahs/2011/AHS_2011_PUF_v1.4_SAS.zip" ]

			# remove the 1983 sas file, which isn't there.
			precise.files <-
				precise.files[ precise.files != 'https://www2.census.gov/programs-surveys/ahs/1983/AHS_1983/AHS_1983_Metro_PUF_SAS.zip' ]
			
			# remove the 1999 table specifications, which doesn't unzip cleanly and serves no purpose
			precise.files <-
				precise.files[ precise.files != 'http://www2.census.gov/programs-surveys/ahs/1999/AHS%201999%20Table%20Specifications.zip' ]
			
			# remove duplicates
			precise.files <- unique( precise.files )
				
			# if there are multiple versions of the csv public use file..
			if( sum( pfl <- grepl( 'PUF_v[0-9]\\.[0-9]' , precise.files ) ) > 1 ){

				# determine all versions available in the current archive
				versions <- gsub( '(.*)PUF_v([0-9]\\.[0-9])_CSV(.*)' , '\\2' , precise.files[ pfl ] )

				# figure out which version to keep
				vtk <- as.character( max( as.numeric( versions ) ) )
				
				# overwrite the `precise.files` vector
				# with a subset..throwing out all lower puf versions.
				precise.files <- precise.files[ !( pfl & !grepl( vtk , precise.files ) ) ]
				# now the program should only download the most current csv version hosted.
				
			}

			if( length( precise.files ) > 0 ){
				catalog <-
					rbind(
						catalog ,
						data.frame(
							year = year ,
							full_url = precise.files ,
							output_folder = paste0( output_dir , '/' , year , '/' ) ,
							stringsAsFactors = FALSE
						)
					)
			}
			
		}

		catalog

	}


lodown_ahs <-
	function( data_name = "ahs" , catalog , ... ){

		on.exit( print( catalog ) )
	
		for ( i in seq_len( nrow( catalog ) ) ){
				
			tf <- tempfile()

			# clear up the `data.loaded` object
			data.loaded <- NULL
			
			# download the exact file to the local disk
			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			# import files from largest to smallest
			tf <- tf[ rev( order( file.info(tf)$size ) ) ]

			# extract the filename (without extension)
			prefix <- gsub( "\\.(.*)" , "" , basename( catalog[ i , 'full_url' ] ) )
			
			# if the filename contains something like `AHS_YYYY_`,
			# remove that text as well
			prefix.without.ahs.year <- 
				gsub( 
					paste0(
						"AHS_" ,
						catalog[ i , 'year' ] ,
						"_"
					) ,
					"" ,
					prefix
				)
				
			# from this point forward, prefix should be lowercase
			# since it won't affect any filenames
			prefix <- tolower( prefix )
			
			# figure out the file extension of what you've downloaded
			extension <- tools::file_ext( catalog[ i , 'full_url' ] )
			
			# clear out the previous temporary filename, just in case.
			previous.tf <- NULL
			
			# if the file is a zipped file..
			if ( tolower( extension ) %in% 'zip' ){
			
				# unzip it to the temporary directory,
				# overwriting the single file's filepath
				# with all of the filepaths in a multi-element character vector
				tf <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )
				
				# once again, extract all file extensions to the local disk
				extension <- tools::file_ext( tf )
				
				# from this point forward, the prefix should be lowercase
				# since it won't affect any filenames
				prefix <- tolower( gsub( "\\.(.*)" , "" , basename( tf ) ) )
				
				# construct the full path to the zipped file on the local disk
				zip.path <- 
					paste0(
						"/" ,
						gsub( 
							paste0(
								"(AHS_|AHS%20)" ,
								catalog[ i , 'year' ] ,
								"(_|%20)"
							) ,
							"" ,
							substr( 
								basename( catalog[ i , 'full_url' ] ) ,
								1 ,
								nchar( basename( catalog[ i , 'full_url' ] ) ) - 4
							)
						)
					)
				
				# 2001 has a backward filename that's causing a duplicate to be missed
				zip.path <- gsub( paste0( catalog[ i , 'year' ] , "(_|%20)AHS" ) , "" , zip.path )

				# 1997 has a typo in national
				zip.path <- gsub( "Nationa_" , "National_" , zip.path )
				
				# `_CSV` and `_PUF` and `PUF_` are all unnecessary characters
				zip.path <- gsub( "(_|%20)CSV" , "" , zip.path )
				zip.path <- gsub( "(_|%20)PUF" , "" , zip.path )
				zip.path <- gsub( "PUF(_|%20)" , "" , zip.path )
				# remove them to reduce carpal tunnel
				
				# finally, before actually constructing the folder
				# make sure it's all lower case because i hate holding shift
				# and without spaces, because %20 gets confusing
				zip.path <- tolower( gsub( "( |%20)" , "_" , zip.path ) )
			
			# if the file to-be-extracted is not a zipped file,
			# simply store it in a sub-folder of the current year.
			} else zip.path <- ""

			
			# loop through each of the available files
			for ( this_ext in seq( extension ) ){
			
				# if the current file is an importable format..
				if( tolower( extension[ this_ext ] ) %in% c( 'xpt' , 'sas7bdat' , 'csv' ) ){

					# ..try to import it using the appropriate function
					import.attempt <-
						try( {
						
								if ( tolower( extension[ this_ext ] ) == 'xpt' ) x <- foreign::read.xport( tf[ this_ext ] )
								
								if ( tolower( extension[ this_ext ] )  == 'csv' ) x <- data.frame( readr::read_csv( tf[ this_ext ] , quote = "'" , guess_max = 100000 ) )
								
								if ( tolower( extension[ this_ext ] )  == 'sas7bdat' ) x <- data.frame( haven::read_sas( tf[ this_ext ] ) )

								
								# if the file that's just been imported is a weight file..
								if ( grepl( 'wgt|weight' , prefix[ this_ext ] ) ){
								
									# determine the control column's position
									ccp <- which( tolower( names( x ) ) == 'control' )
									
									# convert all columns except for the `control` column to numeric
									x[ , -ccp ] <- sapply( x[ , -ccp ] , as.numeric )
									
									# overwrite all of those missings with zeroes
									x[ is.na( x ) ] <- 0
								
								# if it's not the weight table
								
								} else {
									# add a column of all ones
									x$one <- 1
									
									# blank out negative fives through negative nines
									for ( j in seq( ncol( x ) ) ) x[ x[ , j ] %in% -5:-9 , j ] <- NA
								}
								
								

							} , 
							silent = TRUE
						)

					# if that attempt succeeded..
					if ( class( import.attempt ) != 'try-error' ){
						
						# create the save-file-path 
						dir.create( paste0( catalog[ i , 'output_folder' ] , zip.path ) , showWarnings = FALSE , recursive = TRUE )
						
						# construct the full filepath for the filename
						this.filename <-
							paste0(
								catalog[ i , 'output_folder' ] ,
								zip.path , 
								"/" ,
								prefix[ this_ext ] ,
								".rds"
							)
						
						# confirm the file isn't a duplicate of some sort
						if ( file.exists( this.filename ) ) stop( 'cannot overwrite files' )
						
						# convert all column names to lowercase
						names( x ) <- tolower( names( x ) )
											
						# save the newly-renamed object as an `.rds` file on the local disk
						saveRDS( x , file = this.filename , compress = FALSE ) ; rm( x ) ; gc()
									
						# confirm that this data file has been loaded.
						data.loaded <- TRUE
				
					} else {
					
						# set the data.loaded flag to false
						data.loaded <- FALSE
					
					}
				
				} else {
				
					# set the data.loaded flag to false
					data.loaded <- FALSE
					
				}

				# if the data file did not get loaded as an `.rds` file..
				if ( !data.loaded ){

					# determine whether one or many files did not get loaded..
					if ( length( tf ) == 1 ){
					
						# construct the full filepath of the original (not `.rds`) file
						this.filename <-
							paste0(
								catalog[ i , 'output_folder' ] ,
								"/" ,
								prefix.without.ahs.year ,
								"." ,
								extension
							)

					} else {
					
						# create another subdirectory..
						dir.create( paste0( catalog[ i , 'output_folder' ] , zip.path ) )
					
						# ..with the filenames of all non-loaded files
						this.filename <-
							paste0(
								catalog[ i , 'output_folder' ] ,
								zip.path ,
								'/' ,
								basename( tf )
							)
							
					}

					# copy all files over to their appropriate filepaths
					file.copy( tf[ this_ext ] , this.filename[ this_ext ] )
					# so now unloaded files get saved on the local disk as well.
			
				}
			
			}
			
			
			# if the microdata contains both a household-level file..
			hhlf <- prefix[ grep( 'hous' , prefix ) ]
			# ..and a replicate weight file
			wgtf <- prefix[ grep( 'wgt|weight' , prefix ) ]
			
			# merge these two tables immediately #
			
			# if there are more than one of either, break the program.
			stopifnot( length( hhlf ) <= 1 & length( wgtf ) <= 1 )
			
			# if both are available..
			if ( length( hhlf ) == 1 & length( wgtf ) == 1 ){

				# perform the merge twice #
				
				# first: using the r data files #
				
				# determine the filepath of the household-level file
				hhlfn <-
					paste0(
						catalog[ i , 'output_folder' ] ,
						zip.path , 
						"/" ,
						hhlf ,
						".rds"
					)

				# determine the filepath of the replicate-weighted file
				wgtfn <-
					paste0(
						catalog[ i , 'output_folder' ] ,
						zip.path , 
						"/" ,
						wgtf ,
						".rds"
					)

				# load both the household-level..
				hhlf_df <- readRDS( hhlfn )
				
				# ..and weights data tables into working memory
				wgtf_df <- readRDS( wgtfn )
				
				# confirm both tables have the same number of records
				stopifnot( nrow( hhlf_df ) == nrow( wgtf_df ) )
				
				# confirm both tables have only one intersecting column name: `control`
				stopifnot( all( intersect( names( hhlf_df ) , names( wgtf_df ) ) %in% c( 'smsa' , 'control' ) ) )
				
				if( 'smsa' %in% names( hhlf_df ) ) hhlf_df$smsa <- as.numeric( hhlf_df$smsa )
				
				# merge these two files together
				x <- merge( hhlf_df , wgtf_df )
				
				# confirm that the resultant `x` table has the same number of records
				# as the household-level data table
				stopifnot( nrow( x ) == nrow( hhlf_df ) ) ; rm( hhlf_df , wgtf_df ) ; gc()
				
				# determine the name of the hhlf+weights object..
				mergef <- paste( hhlf , wgtf , sep = '_' )
				
				# determine the filepath of the merged file
				merge.fp <-
					paste0(
						catalog[ i , 'output_folder' ] ,
						zip.path , 
						"/" ,
						mergef ,
						".rds"
					)
					
				# save the merged file to the local disk as well	
				saveRDS( x , file = merge.fp , compress = FALSE )
								
				# add the number of records to the catalog
				catalog[ i , 'case_count' ] <- nrow( x )

				rm( x ) ; gc()
				# end of data.frame merge #
					
				# delete the temporary files
				suppressWarnings( file.remove( tf ) )

			}
	
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}
	
		on.exit()

		catalog

	}

