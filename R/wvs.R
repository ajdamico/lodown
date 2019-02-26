get_catalog_wvs <-
	function( data_name = "wvs" , output_dir , ... ){

		catalog <- NULL
	
		this_wave <- httr::GET( "http://www.worldvaluessurvey.org/AJDocumentation.jsp?CndWAVE=-1" )
		available_waves <- -1
		wave_test <- 0
		try_next <- TRUE
		
		my_cookie <- wvs_valid_cookie()

		while( try_next ){
			wave_test <- wave_test + 1
			this_wave <- httr::GET( paste0( "http://www.worldvaluessurvey.org/WVSDocumentationWV" , wave_test , ".jsp" ) )
			if( as.numeric( this_wave$all_headers[[1]]$headers$`content-length` ) > 0 ){

				available_waves <- c( available_waves , wave_test )
				try_next <- TRUE
			} else try_next <- FALSE
		}

		# loop through each wave requested by the user
		for ( this.wave in available_waves ){

			# specify..
			this.dir <- ifelse( this.wave == -1 , "./longitudinal" , paste( "./wave" , this.wave ) )

			if( this.wave == -1 ) dl_page <- "http://www.worldvaluessurvey.org/AJDocumentationSmpl.jsp?CndWAVE=-1&SAID=-1&INID=" else dl_page <- paste0( "http://www.worldvaluessurvey.org/AJDocumentationSmpl.jsp?CndWAVE=" , this.wave )

			# determine the integrated (all-country) files available for download
			agg <- readLines( dl_page, warn = FALSE )

			# determine which of those links are on a line with the text 'Download'
			dlid <- gsub( "(.*)DocDownload(License)?\\('(.*)'\\)(.*)" , "\\3" , grep( "DocDownload(License)?\\('" , agg , value = TRUE ) )

			# find country-specific files as well
			if( this.wave > -1 ){

			  countries <- wvs_appreq( paste0( "http://www.worldvaluessurvey.org/AJDocumentation.jsp?CndWAVE=" , this.wave , "&COUNTRY=" ), the_cookie =  my_cookie)
			  countries <- unlist( strsplit( httr::content(countries, "text"), "\\n|\\t" ) )

				# determine which of those table identifiers lead to actual files
				table_ids <- gsub( '(.*)tr id=\\"(.*)\\" >(.*)' , "\\2" , grep( "tr id" , countries , value = TRUE ) )

				# remove zeroes
				table_ids <- table_ids[ table_ids != "0" ]

				for( this_country in table_ids ){

					# read the country-specific download page
				  dl_page <- wvs_appreq( paste0( "http://www.worldvaluessurvey.org/AJDocumentationSmpl.jsp?CndWAVE=" , this.wave , "&SAID=" , this_country ), my_cookie )
				  dl_page <- unlist( strsplit( httr::content(dl_page, "text"), "\\n|\\t" ) )

				  # extract the identification number of each file
					dlid <- gsub( "(.*)DocDownload(License)?\\('(.*)'\\)(.*)" , "\\3" , grep( "DocDownload(License)?\\('" , dl_page , value = TRUE ) )

					catalog <-
						rbind(
							catalog ,
							data.frame(
								wave = this.wave ,
								this_id = dlid ,
								stringsAsFactors = FALSE
							)
						)

				}

			} else {

				catalog <-
					rbind(
						catalog ,
						data.frame(
							wave = this.wave ,
							this_id = gsub( "(.*)\\('([0-9]*)'\\)" , "\\2" , dlid ) ,
							stringsAsFactors = FALSE
						)
					)

			}

		}

		for( this_entry in seq( nrow( catalog ) ) ){

			if( this_entry == 1 || catalog[ this_entry , 'wave' ] != catalog[ this_entry - 1 , 'wave' ] ) cat( paste0( "loading " , data_name , " catalog for wave " , catalog[ this_entry , 'wave' ] , "\r\n\n" ) )

			catalog[ this_entry , 'full_url' ] <- wvs_getFileById( catalog[ this_entry , 'this_id' ] , my_cookie )

		}



		# universally dump sas files
		catalog <- subset( catalog , !grepl( "_sas_" , full_url ) )

		# only grab the r longitudinal file
		catalog <- subset( catalog , !grepl( "Longitudinal" , full_url , ignore.case = TRUE ) | !grepl( "_spss_|_stata_|_sas_" , full_url ) )

		catalog$output_folder <- paste0( output_dir , ifelse( catalog$wave == -1 , "/longitudinal/" , paste0( "/wave " , catalog$wave , "/" ) ) )

		catalog

	}


lodown_wvs <-
	function( data_name = "wvs" , catalog , ... ){

		on.exit( print( catalog ) )

		my_cookie <- wvs_valid_cookie()

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			response <- wvs_appreq( catalog[ i , 'full_url' ] , my_cookie )

			writeBin( response$content , tf )

			# extract the filename from the website's response header
			this_fn <- basename( response$url ) ; rm( response ) ; gc()

			# correct filenames for WVS longitudinal documents as of 2015-05-09
			if(catalog[ i , 'wave' ] == -1 & this_fn == "")	this_fn = "WVS_EVS_Integrated_Dictionary_Codebook v_2014_09_22.xls"

			if(catalog[ i , 'wave' ] == -1 & this_fn == "04-25.xls") this_fn = "WVS_Values Surveys Integrated Dictionary_TimeSeries_v_2014-04-25.xls"


			# if the file is a zipped file..
			if( tools::file_ext( this_fn ) == 'zip' ){

				# unzip it into the local temporary directory
				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

				# confirm that the unzipped file length is one or it is not an rda/dta/sav file
				stopifnot ( length( unzipped_files ) == 1 | !( grepl( 'stata_dta|spss|rdata' , this_fn ) ) )

				# if it's a stata file, import with `read.dta`
				if( grepl( 'stata_dta' , tolower( this_fn ) ) ){

					attempt_one <- try( x <- data.frame( haven::read_dta( unzipped_files ) ) , silent = TRUE )

					if( class( attempt_one ) == 'try-error' ) try( x <- foreign::read.dta( unzipped_files , convert.factors = FALSE ) , silent = TRUE )

				}

				# if it's an spss file, import with `read.spss`
				if( grepl( 'spss' , tolower( this_fn ) ) ){

					attempt_one <- try( x <- data.frame( haven::read_sav( unzipped_files ) ) , silent = TRUE )

					if( class( attempt_one ) == 'try-error' ) try( x <- foreign::read.spss( unzipped_files , to.data.frame = TRUE , use.value.labels = FALSE ) , silent = TRUE )

				}

				# if it's an r data file, hey the work has been done for you!
				if( grepl( 'rdata' , tolower( this_fn ) ) ){

					# store all loaded object names into `dfn`
					dfn <- load( unzipped_files )

					# if multiple objects were loaded..  check their `class`
					dfc <- sapply( dfn , function( z ) class( get( z ) ) )

					# confirm only one `data.frame` object exists
					stopifnot( sum( dfc == 'data.frame' ) == 1 )

					# store that object into `x`
					x <- get( dfn[ dfc == 'data.frame' ] )

				} else dfn <- NULL

				# if a data.frame object has been imported..
				if( exists( 'x' ) ){

					# convert all column names to lowercase
					names( x ) <- tolower( names( x ) )

					# determine the filepath to store this data.frame object on the local disk
					# if it was "thisfile.sav" then make it "yourdirectory/subdirectory/thisfile.rds"
					rfn <- paste0( catalog[ i , 'output_folder' ] , "/" , gsub( tools::file_ext( this_fn ) , "rds" , this_fn ) )

					# store the data.frame object on the local disk
					saveRDS( x , file = rfn , compress = FALSE )

					catalog[ i , 'case_count' ] <- nrow( x )

				} else file.copy( unzipped_files , paste0( catalog[ i , 'output_folder' ] , "/" , basename( unzipped_files ) ) )

				suppressWarnings( rm( x ) )	; gc()

				suppressWarnings( rm( list = dfn ) )

			} else {

				# copy the temporary file over to the current subdirectory
				file.copy( tf , paste0( catalog[ i , 'output_folder' ] , "/" , this_fn ) )

			}

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()

		catalog

	}



# http://stackoverflow.com/questions/40498277/programmatically-scraping-a-response-header-within-r
# http://stackoverflow.com/questions/38156180/how-to-download-a-file-behind-a-semi-broken-javascript-asp-function-with-r


# determine the full url of a WVS file based on the file id
wvs_getFileById <-
	function(fileId,cookie) {

		response <- httr::GET(
			url = "http://www.worldvaluessurvey.org/jdsStatJD.jsp?ID=2.72.48.149%09IT%09undefined%0941.8902%2C12.4923%09Lazio%09Roma%09Orange%20SA%20Telecommunications%20Corporation&url=http%3A%2F%2Fwww.worldvaluessurvey.org%2FAJDocumentation.jsp&referer=null&cms=Documentation",
			httr::add_headers(
				`Accept` = "*/*",
				`Accept-Encoding` = "gzip, deflate",
				`Accept-Language` = "en-US,en;q=0.8",
				`Cache-Control` = "max-age=0",
				`Connection` = "keep-alive",
				`X-Requested-With` = "XMLHttpRequest",
				`Host` = "www.worldvaluessurvey.org",
				`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
				`Content-type` = "application/x-www-form-urlencoded",
				`Referer` = "http://www.worldvaluessurvey.org/AJDocumentation.jsp?CndWAVE=-1",
				`Cookie` = cookie))

		post_data <- list(
			ulthost = "WVS",
			CMSID = "",
			CndWAVE = "-1",
			SAID = "-1",
			DOID = fileId,
			AJArchive = "WVS Data Archive",
			EdFunction = "",
			DOP = "",
			PUB = "")

		response <- httr::POST(
			url = "http://www.worldvaluessurvey.org/AJDownload.jsp",
			httr::config(followlocation = FALSE),
			httr::add_headers(
				`Accept` = "*/*",
				`Accept-Encoding` = "gzip, deflate",
				`Accept-Language` = "en-US,en;q=0.8",
				`Cache-Control` = "max-age=0",
				`Connection` = "keep-alive",
				`Host` = "www.worldvaluessurvey.org",
				`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
				`Content-type` = "application/x-www-form-urlencoded",
				`Referer` = "http://www.worldvaluessurvey.org/AJDocumentation.jsp?CndWAVE=-1",
				`Cookie` = cookie),
			body = post_data,
			encode = "form")

		location <- httr::headers(response)$location
		location
	}


wvs_valid_cookie <-
	function(){

		httr::handle_reset( "http://www.worldvaluessurvey.org/AJDocumentation.jsp?CndWAVE=-1" )

		# request a valid cookie from the server and then don't touch it
		response <- httr::GET(
			url = "http://www.worldvaluessurvey.org/AJDocumentation.jsp?CndWAVE=-1",
			httr::add_headers(
				`Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
				`Accept-Encoding` = "gzip, deflate",
				`Accept-Language` = "en-US,en;q=0.8",
				`Cache-Control` = "max-age=0",
				`Connection` = "keep-alive",
				`Host` = "www.worldvaluessurvey.org",
				`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
				`Content-type` = "application/x-www-form-urlencoded",
				`Referer` = "http://www.worldvaluessurvey.org/AJDownloadLicense.jsp",
				`Upgrade-Insecure-Requests` = "1"))

		set_cookie <- httr::headers(response)$`set-cookie`
		cookies <- strsplit(set_cookie, ';')
		cookies[[1]][1]
	}


wvs_appreq <-
	function( this_url , the_cookie ){
		httr::VERB(verb = "GET", url = this_url ,
		httr::add_headers(Accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			`Accept-Encoding` = "gzip, deflate", `Accept-Language` = "en-US,en;q=0.5",
			Connection = "keep-alive", Host = "www.worldvaluessurvey.org",
			Referer = "http://www.worldvaluessurvey.org/AJDocumentationSmpl.jsp",
			`Upgrade-Insecure-Requests` = "1", `User-Agent` = "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0"),
		httr::set_cookies(JSESSIONID = gsub( "JSESSIONID=" , "" , the_cookie )) )
	}
