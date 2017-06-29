get_catalog_nls <-
	function( data_name = "nls" , output_dir , ... ){

		stop( "fix with https://www.nlsinfo.org/accessing-data-cohorts" )
	
		catalog <- NULL
		
		# open the jsp box
		httr::GET( "https://www.nlsinfo.org/investigator/pages/search.jsp" )

		# log on to the nlsinfo investigator and pull all available studies
		studies <- httr::GET( "https://www.nlsinfo.org/investigator/servlet1?get=STUDIES&t=1" )

		# extract the study names option
		study_values <- XML::xpathSApply( XML::htmlParse( httr::content( studies ) ) , "//option" , XML::xmlAttrs )

		study_text <- XML::xpathSApply( XML::htmlParse( httr::content( studies ) ) , "//option" , XML::xmlValue )

		study_text <- stringr::str_trim( study_text[ study_values != "-1" ] )
		
		study_values <- as.character( study_values[ study_values != "-1" ] )
	
			
		# loop through all designated studies
		for ( this_study in seq_along( study_values ) ){

			# determine all available substudies within the selected studies
			substudies <- httr::GET( paste0( "https://www.nlsinfo.org/investigator/servlet1?get=SUBSTUDIES&study=" , study_values[ this_study ] ) )
			
			# extract the option tags from the substudies page
			substudy_values <- XML::xpathSApply( XML::htmlParse( httr::content( substudies ) ) , "//option" , XML::xmlAttrs )
			
			# also extract the values contained within those tags
			substudy_text <- XML::xpathSApply( XML::htmlParse( httr::content( substudies ) ) , "//option" , XML::xmlValue )
			
			# convert that list into a vector
			substudy_values <- unlist( substudy_values )
			
			# limit the substudies to only actual values, not dropdown list identifiers
			substudy_values <- as.character( substudy_values[ names( substudy_values ) == 'value' ] )
			
			# just like the overall study names, remove the default negative one selection
			# from both names..
			substudy_text <- substudy_text[ substudy_values != "-1" ]
			# ..and numbers
			substudy_values <- substudy_values[ substudy_values != "-1" ]
			
			# remove leading and trailing spaces from the substudy names
			substudy_text <- stringr::str_trim( substudy_text )

			catalog <-
				rbind(
					catalog ,
					data.frame(
						study_name = study_text[ this_study ] ,
						study_value = study_values[ this_study ] ,
						substudy_name = substudy_text ,
						substudy_value = substudy_values ,
						output_folder = paste0( output_dir , "/" , substudy_text , "/" ) ,
						stringsAsFactors = FALSE
					)
				)
		
		}
				
		catalog

	}


lodown_nls <-
	function( data_name = "nls" , catalog , ... ){

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the strata and psu for studies where they're available
			if( catalog[ i , 'study_value' ] == "NLSY97" ){
			
				# download the nlsy 1997 cohort's sampling information
				cachaca( "https://www.nlsinfo.org/sites/nlsinfo.org/files/attachments/140618/nlsy97stratumpsu.zip" , tf , mode = 'wb' )
				
				# unzip to the local disk
				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) )

				strpsu <- read.csv( unzipped_files[ grep( '\\.csv' , unzipped_files ) ] )
				
				# store the complex sample variables on the local disk
				saveRDS( strpsu , file = paste0( catalog[ i , 'output_folder' ] , "/strpsu.rds" ) )
				
				# delete the temporary files
				suppressWarnings( file.remove( tf , unzipped_files ) )
				
			}


			# set the nls investigator to allow downloads for this substudy
			httr::GET( paste0( "https://www.nlsinfo.org/investigator/servlet1?set=STUDY&id=" , catalog[ i , 'substudy_value' ] ) )

			# identify all possible extract files for downloading
			z <- httr::GET( "https://www.nlsinfo.org/investigator/servlet1?get=SEARCHVALUES&type=RNUM" )

			# convert this rnum html result into an xml-readable object
			doc <- XML::htmlParse( z )
			
			# extract all options from this xml text
			opts <- XML::getNodeSet( doc , "//select/option" )
			
			# extract all rnum values from within the xml
			all.option.values <- sapply( opts , XML::xmlGetAttr , "value" )
			
			# remove any negative ones, since those are not rnums themselves
			all.option.values <- all.option.values[ all.option.values != "-1" ]

			## get list of already download files (in case you've had to start and then stop)
			already.stored <- lapply( list.files( catalog[ i , 'output_folder' ] ), FUN = function(x) strsplit(x, split = '\\.')[[1]][1])

			## unlist
			already.stored <- unlist(already.stored)

			## subset so we only download what hasn't already been downloaded
			all.option.values <- all.option.values[!(all.option.values %in% already.stored)]

			# loop through each available rnum extract
			for ( option.value in all.option.values ){

				# initiate a counter
				attempt.count <- 0
				
				# start off the `attempt` object as an error.
				attempt <- try( stop() , silent = TRUE )
				# this is only useful at the start of this next `while` command
				
				# so long as the `attempt` object is an error..
				while( class( attempt ) == 'try-error' ){
					
					# add one to the counter
					attempt.count <- attempt.count + 1
				
					# display any actual errors for the user
					if ( attempt.count > 1 ) print( attempt )
				
					# after the fifth attempt, shut down the program.
					if ( attempt.count > 5 ) stop( "tried five times with no luck.  peace out." )
				
					# overwrite the `attempt` object with the result of..
					attempt <-
						try( {
							
							# re-set the nls investigator to download only the default-included variables
							httr::GET( paste0( "https://www.nlsinfo.org/investigator/servlet1?set=STUDY&id=" , catalog[ i , 'substudy_value' ] , "&reset=true" ) )

							# initiate the server for downloads
							httr::GET( "https://www.nlsinfo.org/investigator/servlet1?set=preference&pref=all" )

							# specify the rnum extract to download
							httr::GET( paste0( "https://www.nlsinfo.org/investigator/servlet1?get=Results&xml=true&criteria=RNUM%7CSW%7C" , option.value , "&sortKey=RNUM&sortOrder=ascending&&PUBID=noid&limit=all" ) )

							# rather than downloading only recommended variables, download all of 'em
							httr::GET( "https://www.nlsinfo.org/investigator/servlet1?set=tagset&select=all&value=true" )

							# specify that only the csv file should be downloaded
							job.char <- httr::GET( "https://www.nlsinfo.org/investigator/servlet1?collection=on&sas=off&spss=off&stata=off&codebook=on&csv=on&event=start&cmd=extract&desc=default" )

							# extract the specific job id from the above result
							job.id <- gsub( 'job:' , '' , as.character( job.char ) )

							# trigger the creation of the extract
							httr::GET( "https://www.nlsinfo.org/investigator/servlet1?get=downloads&study=current" )

							# start out with a blank string
							v <- ""
							
							# so long as the `v` string does not contain this response text..
							while( !( grepl( "\"message\":\"\"}}" , as.character( v ) , fixed = TRUE ) ) ){
								
								# ping the server to determine the current progress of the creation of the current extract
								v <- httr::GET( paste0( "https://www.nlsinfo.org/investigator/servlet1?job=" , job.id , "&event=progress&cmd=extract&_=" , as.numeric( Sys.time() ) * 1000 ) )
							
								# if the download hits an error, break out of the current loop.
								ep <- FALSE
								
								# see if the current page contains an error page text, instead of actual data.
								try( ep <- XML::xpathSApply( XML::htmlParse( v , asText = TRUE ) , '//title' , XML::xmlValue ) == 'Error Page' , silent = TRUE )
								
								# if it does contain an error page, break the program inside this current try loop
								if( ( length( ep ) > 0 ) && ( ep ) ) stop( "Error Page" )
								# first successful usage of `&&` operator.  pat on the back.
							
								# extract the current contents of the `v` object to determine the current progress
								msg <- strsplit( strsplit( as.character(v) , 'message\":\"' )[[1]][2] , '\"}}' )[[1]][1]
								
								# print that progress to the screen
								cat( "    " , msg , "\r\n\n" )
								
								# give the progress bar fifteen seconds before it
								# refreshes so it's not overloading the website
								Sys.sleep( 15 )
								
							}
							# once the extract creation has been completed
							
							# initiate an empty `u` object..
							u <- NULL
							
							# ..with a failed-error code
							u$headers$status <- 500

							# initiate a timer
							start.time <- Sys.time()
							
							# so long as the status code returns unfinished
							while( !is.null( u$headers$status ) && u$headers$status == 500 ){

								# if you've been waiting more than two minutes, just stop.
								if ( Sys.time() - start.time > 120 ) stop( 'waited two minutes after extract created, still no download' )
							
								# download the zipped file for this specific job id
								u <- httr::GET( paste0( "https://www.nlsinfo.org/investigator/downloads/" , job.id , "/default.zip" ) )
								
							}

							# save that result zipped file into the temporary file on your local disk
							writeBin( httr::content( u , "raw" ) , tf )
							
							# unzip that temporary file into the temporary directory on your local disk
							unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

							# determine the location of the `.csv` file within the zipped file you've just unarchived
							csv <- unzipped_files[ grep( '.csv' , unzipped_files , fixed = TRUE ) ]

							# save that zipped file as a data.frame
							assign( "x" , read.csv( csv ) )

							# store in the current save-location
							saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , option.value , ".rds" ) )

							catalog[ i , 'case_count' ] <- nrow( x )
							
							# delete the temporary files
							suppressWarnings( file.remove( tf , unzipped_files ) )

							cat( paste0( data_name , " " , which( option.value == all.option.values ) , " of " , length( all.option.values ), " extract " , option.value , " attempt " , attempt.count , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

						} , 
						silent = TRUE 
					)
					
					# wait the same number of minutes as you have attempted-counted,
					# but after the last attempt, don't wait at all.
					if( class( attempt ) == 'try-error' ) Sys.sleep( 60 * ifelse( attempt.count >= 5 , 0 , attempt.count ) ) else Sys.sleep( 15 )
						
				}
				
			}
					
		
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		catalog

	}

