get_catalog_nsduh <-
	function( data_name = "nsduh" , output_dir , ... ){

	catalog <- get_catalog_icpsr( "00064" , bundle_preference = "stata" )
	
	catalog$unzip_folder <- paste0( output_dir , "/" , catalog$temporalCoverage , ifelse( catalog$dataset_name %in% c( "Part A" , "Part B" ) , paste0( "/" , tolower( catalog$dataset_name ) ) , "" ) )

	catalog$output_filename <- paste0( output_dir , "/" , catalog$temporalCoverage , " " , ifelse( catalog$dataset_name %in% c( "Part A" , "Part B" ) , tolower( catalog$dataset_name ) , "main" ) , ".rda" )

	catalog

}


lodown_nsduh <-
	function( data_name = "nsduh" , catalog , ... ){
	
		lodown_icpsr( data_name = data_name , catalog , ... )

		for( i in seq_len( nrow( catalog ) ) ){
		
			# find stata file within unzipped path
			stata_files <- grep( "\\.dta$" , list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )
			
			for( this_stata in stata_files ){
			
				x <- data.frame( haven::read_dta( this_stata ) )
				
				# path to the supplemental recodes file
				path_to_supp <- grep( "\\Supplemental_syntax\\.do$" , list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )

				# read the supplemental recodes lines into R
				commented.supp.syntax <- readLines( path_to_supp )

				# and remove any stata comments
				uncommented.supp.syntax <- SAScii::SAS.uncomment( commented.supp.syntax , "/*" , "*/" )

				# remove blank lines
				supp.syntax <- uncommented.supp.syntax[ uncommented.supp.syntax != "" ]

				# confirm all remaining recode lines contain the word 'replace'
				# right now, the supplemental recodes are relatively straightforward.
				# should any of them contain non-'replace' syntax, this part of this
				# R script will require more flexibility
				stopifnot( 
					length( supp.syntax ) == 
					sum( unlist( lapply( "replace" , grepl , supp.syntax ) ) )
				)

				# figure out exactly how many recodes will need to be processed
				# (this variable will be used for the progress monitor that prints to the screen)
				how.many.recodes <- length( supp.syntax )
				
				# loop through the entire stata supplemental recodes file
				for ( j in seq( supp.syntax ) ){

					# isolate the current stata "replace .. if .." command
					current.replacement <- supp.syntax[ j ]
					
					# locate the name of the current variable to be overwritten
					space.positions <- 
						gregexpr( 
							" " , 
							current.replacement 
						)[[1]]
					
					variable <- substr( current.replacement , space.positions[1] + 1 , space.positions[2] - 1 )
					
					# figure out the logical test contained after the stata 'if' parameter
					condition.to.blank <- unlist( strsplit( current.replacement , " if " ) )[2]
					
					# add an x$ to indicate which data frame to alter in R
					condition.test <- gsub( variable , paste0( "x$" , variable ) , condition.to.blank )
					
					# build the entire recode line, with a "<- NA" to overwrite
					# each of these codes with missing values
					recode.line <- 
						paste0( 
							"x[ no.na( " , 
							condition.test ,
							") , '" ,
							variable ,
							"' ] <- NA"
						)
						
					# uncomment this to print the current recode to the screen
					# print( recode.line )
					
					# execute the actual recode
					eval( parse( text = recode.line ) )
					
				}

				names( x ) <- tolower( names( x ) )
				
				save( x , file = catalog[ i , 'output_filename' ] )
				
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
		
			}
		
		}
		
		invisible( TRUE )

	}

