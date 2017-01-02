

# from http://stackoverflow.com/questions/16474696/read-system-tmp-dir-in-r
gettmpdir <-
	function() {
		tm <- Sys.getenv(c('TMPDIR', 'TMP', 'TEMP'))
		d <- which(file.info(tm)$isdir & file.access(tm, 2) == 0)
		if (length(d) > 0)
		tm[[d[1]]]
		else if (.Platform$OS.type == 'windows')
		Sys.getenv('R_USER')
		else '/tmp'
	}


rcurl_filesize <-
	function( url ){
		xx <- RCurl::getURL(url, nobody=1L, header=1L)
		yy <- strsplit(xx, "\r\n")[[1]]
		as.numeric( gsub( "Content-Length: " , "" , grep( "Content-Length" , yy , value = TRUE ) ) )
	}

httr_filesize <-
	function( url ){
		xx <- httr::HEAD(url)
		yy <- httr::headers(xx)$`content-length`
		as.numeric( yy )
	}

download_get_filename <- function(url, curl=RCurl::getCurlHandle(), ...) {
		h <- RCurl::basicHeaderGatherer()
		RCurl::curlSetOpt(nobody=T, curl=curl)
		RCurl::getURL(url, headerfunction = h$update, curl=curl, ...)
		gsub( '(.*)\\"(.*)\\"' , "\\2" , h$value()[["Content-Type"]])
	}

download_to_filename <- function(url, dlfile, curl=RCurl::getCurlHandle(), ...) {
		RCurl::curlSetOpt(nobody=F, httpget=T, curl=curl)
		writeBin(RCurl::getBinaryURL(url = url, curl=curl, ...), dlfile)
		0L
	}

#' best file download, all other file downloads have inferior potassium
#'
#' check remote file size. check cache for a match and if it exists, copy it over. otherwise, download the file, confirm file size, save to cache too
#'
#' @param this_url where to download from
#' @param destfile where to save locally, unnecessary when returning an object with \code{httr::GET} or \code{RCurl::getBinaryURL}
#' @param ... passed to FUN
#' @param FUN defaults to \code{download.file} but \code{downloader::download}, \code{httr::GET}, \code{RCurl::getBinaryURL} also work
#' @param attempts number of times to retry a broken download
#' @param sleepsec length of \code{Sys.sleep()} between broken downloads
#' @param filesize_fun use \code{RCurl::getURL} or \code{httr::HEAD} to determine file size.  use "unzip_verify" to verify the download by unzipping it without a warning instead
#'
#' @return just pass on whatever FUN returns
#'
#' @examples
#'
#' \dontrun{
#'
#' tf <- tempfile()
#' cachaca( "https://www.r-project.org/logo/Rlogo.png" , tf , mode = 'wb' )
#' cachaca( "https://www.r-project.org/logo/Rlogo.png" , tf , mode = 'wb' )
#' 
#' }
#'
#' @export
cachaca <-
	function (

		this_url ,

		destfile = NULL ,

		# pass in any other arguments needed for the FUN
		... ,

		# specify which download function to use.
		# `download.file` and `downloader::download` should both work.
		FUN = download.file ,

		# how many attempts should be made with FUN?
		attempts = 3 ,
		# just in case of a server timeout or smthn equally annoying

		# how long should cachaca wait between attempts?
		sleepsec = 60 ,

		# which filesize function should be used
		# c( 'rcurl' , 'httr' )
		filesize_fun = 'rcurl'

	) {


		# if the cached file exists, assume it's good.
		urlhash <- digest::digest(this_url)

		cachefile <-
			paste0(
				gsub( "\\" , "/" , gettmpdir() , fixed = TRUE ) ,
				"/" ,
				urlhash ,
				".Rcache"
			)

		if( file.exists( cachefile ) ){

			if( is.null( destfile ) ){

				load( cachefile )

				cat( paste0( "'" , this_url , "'\r\ncached in\r\n'" , cachefile , "'\r\nreturning loaded object within R session\r\n\n" ) )

				return( invisible( success ) )

			} else {

				cat( paste0( "'" , this_url , "'\r\ncached in\r\n'" , cachefile , "'\r\ncopying to\r\n'" , destfile , "'\r\n\n" ) )

				return( invisible( ifelse( file.copy( cachefile , destfile , overwrite = TRUE ) , 0 , 1 ) ) )

			}

		}


		if( is.null( destfile ) ){
			cat( paste0( "saving from URL\r\n'" , this_url , "'\r\nto loaded object within R session\r\n\n" ) )
		} else {
			cat( paste0( "Downloading from URL\r\n'" , this_url , "'\r\nto file\r\n'" , destfile , "'\r\n\n" ) )
		}

		
		if( filesize_fun == 'rcurl' ) this_filesize <- rcurl_filesize( this_url )

		if( filesize_fun == 'httr' ) this_filesize <- httr_filesize( this_url )

		if( filesize_fun != 'unzip_verify' && this_filesize == 0 ) stop( "remote server lists file size as zero" )

		# start out with a failed attempt, so the while loop below commences
		failed.attempt <- try( stop() , silent = TRUE )

		# keep trying the download until you run out of attempts
		# and all previous attempts have failed

		initial.attempts <- attempts

		while( attempts > 0 & class( failed.attempt ) == 'try-error' ){

			# only run this loop a few times..
			attempts <- attempts - 1

			failed.attempt <-
				try( {

					# if there is no destination file, then `success` contains the data.
					if( is.null( destfile ) ){

						# did the download work?
						success <- do.call( FUN , list( this_url , ... ) )

						if( filesize_fun == 'unzip_verify' ){
						
							unzip_tf <- tempfile()
							
							writeBin( httr::content( success ) , unzip_tf )
						
							tryCatch( unzipped_files <- unzip( unzip_tf , exdir = paste0( tempdir() , "/unzips" ) ) , warning = function(w) { stop( "unzip_verify failed: " , conditionMessage( w ) ) } )
							
							# if the unzip worked without issue, then the file size is correct
							this_filesize <- file.info( destfile )$size
							
							file.remove( unzip_tf , unzipped_files )
													
						}
												
						if( !isTRUE( all.equal( length( success ) , this_filesize ) ) && !isTRUE( all.equal( length( httr::content( success ) ) , this_filesize ) ) ){

							message( paste0( "downloaded binary url size (" , length( success ) , ") does not match server's content length (" , this_filesize , ")" ) )

							class(failed.attempt) <- 'try-error'

							rm( success )

						}

					} else {

						# did the download work?
						success <- do.call( FUN , list( this_url , destfile , ... ) ) == 0

						if( filesize_fun == 'unzip_verify' ){
						
							tryCatch( unzipped_files <- unzip( destfile , exdir = paste0( tempdir() , "/unzips" ) ) , warning = function(w) { stop( "unzip_verify failed: " , conditionMessage( w ) ) } )
							
							# if the unzip worked without issue, then the file size is correct
							this_filesize <- file.info( destfile )$size
							
							file.remove( unzipped_files )
													
						}
						
						if( !isTRUE( all.equal( file.info( destfile )$size , this_filesize ) ) ){

							message( paste0( "downloaded file size on disk (" , file.info( destfile )$size , ") does not match server's content length (" , this_filesize , ")" ) )

							class(failed.attempt) <- 'try-error'

							rm( success )

						}

					}
		
					} , silent = TRUE
				)

			# if the download did not work, wait `sleepsec` seconds and try again.
			if( class( failed.attempt ) == 'try-error' ){
				cat( paste0( "download issue with\r\n'" , this_url , "'\r\n\n" ) )
				Sys.sleep( sleepsec )
			}

		}

		# double-check that the `success` object exists.. it might not if `attempts` was set to zero.
		if ( exists( 'success' ) ){

		if( is.null( destfile ) ){

		save( success , file = cachefile )

		} else file.copy( destfile , cachefile , overwrite = TRUE )

		return( invisible( success ) )

		# otherwise break.
		} else stop( paste( "download failed after" , initial.attempts , "attempts" ) )

	}
