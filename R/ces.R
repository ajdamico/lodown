get_catalog_ces <-
	function( data_name = "ces" , output_dir , ... ){

	all_links <- rvest::html_attr( rvest::html_nodes( xml2::read_html( "https://www.bls.gov/cex/pumd_data.htm#stata" ) , "a" ) , "href" )

	stata_links <- grep( "stata(.*)\\.zip" , all_links , value = TRUE , ignore.case = TRUE )
	
	stata_years <- gsub( "(.*)([0-9][0-9])(.*)" , "\\2" , basename( stata_links ) )
	
	catalog <-
		data.frame(
			type = gsub( "([0-9][0-9])(.*)" , "" , basename( stata_links ) ) ,
			year = ifelse( as.numeric( stata_years ) > 95 , 1900 + as.numeric( stata_years ) , 2000 + as.numeric( stata_years ) ) ,
			full_url = paste0( 'https://www.bls.gov/' , stata_links ) ,
			stringsAsFactors = FALSE
		)
		
	catalog$output_folder <- paste0( output_dir , "/" , catalog$year , "/" )

	catalog

}


lodown_ces <-
	function( data_name = "ces" , catalog , ... ){

		on.exit( print( catalog ) )

		all_thresholds <- census_thresholds()
	
		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# identify dta files
			dta_files <- unzipped_files[ grep( '\\.dta' , unzipped_files ) ]

			df_names <- NULL
			
			# unique ids
			newids <- NULL
			
			# loop through a character vector containing the complete filepath
			# of each of the dta files downloaded to the local disk..
			for ( this_dta in dta_files ){

				df_name <- gsub( "(.*)\\.(.*)" , "\\1" , basename( this_dta ) )
			
				# read the current stata-readable (.dta) file into R
				x <- data.frame( haven::read_dta( this_dta ) )

				# if the data.frame is a family file, tack on poverty thresholds
				if( grepl( "fmli" , df_name ) ){

					# subset the complete threshold data down to only the current year
					thresh_merge <- all_thresholds[ all_thresholds$year == catalog[ i , "year" ] , ]

					# remove the `year` column
					thresh_merge$year <- NULL

					# rename fields so they merge cleanly
					names( thresh_merge ) <- c( 'family_type' , 'num_kids' , 'poverty_threshold' )

					x$num_kids <- ifelse( x$perslt18 > 8 , 8 , x$perslt18 )
					x$num_kids <- ifelse( x$num_kids == x$fam_size , x$fam_size - 1 , x$num_kids )

					# re-categorize family sizes to match census groups
					x$family_type <-
						ifelse( x$fam_size == 1 & x$age_ref < 65 , "Under 65 years" ,
						ifelse( x$fam_size == 1 & x$age_ref >= 65 , "65 years and over" ,
						ifelse( x$fam_size == 2 & x$age_ref < 65 , "Householder under 65 years" ,
						ifelse( x$fam_size == 2 & x$age_ref >= 65 , "Householder 65 years and over" ,
						ifelse( x$fam_size == 3 , "Three people" , 
						ifelse( x$fam_size == 4 , "Four people" , 
						ifelse( x$fam_size == 5 , "Five people" , 
						ifelse( x$fam_size == 6 , "Six people" , 
						ifelse( x$fam_size == 7 , "Seven people" , 
						ifelse( x$fam_size == 8 , "Eight people" , 
						ifelse( x$fam_size >= 9 , "Nine people or more" , NA ) ) ) ) ) ) ) ) ) ) )

					# merge on the `poverty_threshold` variable while
					# confirming no records were tossed
					before_nrow <- nrow( x )

					x <- merge( x , thresh_merge )

					stopifnot( nrow( x ) == before_nrow )

				}

				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )

				newids <- unique( c( newids , x$newid ) )
				
				# save the file as an R data file (.rds) immediately
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , df_name , ".rds" ) , compress = FALSE )
				
			}

			catalog[ i , 'case_count' ] <- length( newids )
			
			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

