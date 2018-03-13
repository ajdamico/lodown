#' locally download, import, prepare icpsr microdata
#'
#' get_catalog_icpsr retrieves a listing of all available extracts for a microdata set
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog \code{data.frame} detailing available microdata extracts
#' @param series_number any data series number from http://www.icpsr.umich.edu/icpsrweb/ICPSR/series
#' @param study_numbers one or more study numbers from http://www.icpsr.umich.edu/icpsrweb/ICPSR/studies?q=
#' @param archive defaults to "ICPSR" but other archives include "HMCA" , "NACJD" , "NAHDAP" , "NADAC" , "civicleads" , "RCMD" , "ADDEP" , "childcare" , "NCAA" , "NACDA" , "DSDR" , "METLDB"
#' @param bundle_preference when multiple filetypes are available, which should be given priority?
#' @param ... passed to \code{get_catalog} and \code{lodown_}
#'
#' @export
lodown_icpsr <-
  function( data_name , catalog , ... ){

		on.exit( print( catalog ) )

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at https://www.icpsr.umich.edu/cgi-bin/newacct" )

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://www.icpsr.umich.edu/cgi-bin/newacct" )

		your_email <- list(...)[["your_email"]]

		your_password <- list(...)[["your_password"]]

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			login <- "https://www.icpsr.umich.edu/rpxlogin"
			terms <- "https://www.icpsr.umich.edu/cgi-bin/terms"
			download_prefix <- "https://www.icpsr.umich.edu/cgi-bin/bob/zipcart2?"

			values <-
				list(
					agree = "yes",
					path = "ICPSR" ,
					study = catalog[ i , 'study_number' ] ,
					ds = catalog[ i , 'dsNo' ] ,
					bundle = catalog[ i , 'bundle' ] ,
					dups = "yes",
					email=your_email,
					password=your_password
				)

			# Accept the terms on the form,
			# generating the appropriate cookies
			httr::POST(terms, body = values)
			httr::POST(login, body = values)
			
			httr::HEAD(
			    catalog[ i , 'full_url' ] ,
			    query = values
			)

			httr::POST(terms, body = values)
			httr::POST(login, body = values)
			
			cachaca(
			    catalog[ i , 'full_url' ] ,
			    destfile = tf ,
			    FUN = httr::GET ,
			    filesize_fun = 'unzip_verify' ,
			    httr::write_disk( tf , overwrite = TRUE ) ,
			    httr::progress() ,
			    query = values
			)

			unzip_warn_fail( tf , exdir = gsub( "/$" , "" , catalog[ i , "unzip_folder" ] ) , junkpaths = TRUE )

			# delete the temporary files
			file.remove( tf )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " unzipped to '" , catalog[ i , 'unzip_folder' ] , "'\r\n\n" ) )

		}

		on.exit()

		catalog

	}




#' @rdname lodown_icpsr
#' @export
get_catalog_icpsr <-
	function( series_number = NULL , study_numbers = NULL , archive = "ICPSR" , bundle_preference = c( "rdata" , "stata" , "sas" , "spss" , "delimited" , "ascsas" , "ascstata" , "ascspss" , "ascii" , "gis" , "doc" ) ){

		if( is.null( study_numbers ) ){

			series_page <- paste0( "https://www.icpsr.umich.edu/icpsrweb/" , archive , "/series/" , series_number )

			series_xml <- xml2::read_html( series_page )

			study_numbers <- gsub( "[^0-9]" , "" , rvest::html_text( rvest::html_nodes( series_xml , ".studyNo" ) ) )

		}

		series_results <- NULL

		for( study_number in study_numbers ){

			study_page <- paste0( "https://pcms.icpsr.umich.edu/pcms/api/1.0/studies/" , study_number , "/files" )
			
			study_datasets <- jsonlite::fromJSON( study_page , simplifyDataFrame = TRUE )

			study_datasets$fileUris <- study_datasets$files <- NULL
		
			available_bundles <- study_datasets[ grep( "^bundles" , names( study_datasets ) ) ][[1]]

			sorted_bundle_preference <- available_bundles[ bundle_preference ]
			
			sorted_bundle_preference[ , 'this_bundle' ] <- NA
			
			for( i in seq( ncol( sorted_bundle_preference ) - 1 ) ){
			
				sorted_bundle_preference[ is.na( sorted_bundle_preference[ , 'this_bundle' ] ) & sorted_bundle_preference[ , i ] , 'this_bundle' ] <-
					names( sorted_bundle_preference )[ i ]
				
			}
			
			if( any( is.na( sorted_bundle_preference[ , 'this_bundle' ] ) ) ){
			
				cat( "removing datasets without matching bundles\r\r\n\n" )
			
				print( study_datasets[ is.na( sorted_bundle_preference[ , 'this_bundle' ] ) , ] )
			
			}
			
			study_datasets$bundle <- sorted_bundle_preference$this_bundle
			
			this_study <- study_datasets[ !is.na( study_datasets[ , 'bundle' ] ) , ]
		
			in_results_not_study <- setdiff( names( series_results ) , names( this_study ) )

			in_study_not_results <- setdiff( names( this_study ) , names( series_results ) )

			this_study[ in_results_not_study ] <- NA

			if( !is.null( series_results ) ) series_results[ in_study_not_results ] <- NA

			this_study$study_number <- study_number

			series_results <- rbind( series_results , this_study )

		}


		series_results[ , 'full_url' ] <- 
			paste0( 
				"https://www.icpsr.umich.edu/cgi-bin/bob/zipcart2?study=" ,
				series_results[ , 'study_number' ] ,
				"&bundle=" ,
				series_results[ , 'bundle' ] ,
				"&ds=" ,
				series_results[ , 'dsNo' ]
			)
		
		
		series_results

	}

icpsr_stata <-
	function( path_to_stata , catalog_entry ){

		x <- data.frame( haven::read_dta( path_to_stata ) )

		# path to the supplemental recodes file
		path_to_supp <- grep( "upplemental( |_|-)syntax\\.do$" , list.files( catalog_entry[ , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )

		# read the supplemental recodes lines into R
		commented.supp.syntax <- readLines( path_to_supp )

		# and remove any stata comments
		uncommented.supp.syntax <- SAScii::SAS.uncomment( commented.supp.syntax , "/*" , "*/" )

		# remove blank lines
		supp.syntax <- stringr::str_trim( uncommented.supp.syntax[ uncommented.supp.syntax != "" ] )

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

		x

	}
