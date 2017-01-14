get_catalog_pirls <-
	function( data_name = "pirls" , output_dir , ... ){

		catalog <-
			data.frame(
				year = seq( 2001 , 2011 , 5 ) ,
				stringsAsFactors = FALSE
			)

		catalog$output_folder <- paste0( output_dir , "/" , catalog$year , "/" )
		
		catalog

	}


lodown_pirls <-
	function( data_name = "pirls" , catalog , ... ){

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

		
			# # # # # # # # # # # #
			# download all files  #
			# # # # # # # # # # # #

			# specify the pathway to each and every spss data set to download.
			ftd <-
				c(
					"http://timssandpirls.bc.edu/pirls2011/downloads/P11_SPSSData_pt1.zip" ,
					"http://timssandpirls.bc.edu/pirls2011/downloads/P11_SPSSData_pt2.zip" ,
					"http://timssandpirls.bc.edu/PDF/PIRLS2006_SPSSData.zip" ,
					"http://timssandpirls.bc.edu/pirls2001i/Pirls2001Database/pirls_2001_spssdata.zip"
				)
				
			# for each file to download..
			for ( j in grep( catalog[ i , 'year' ] , ftd , value = TRUE ) ){
				
				# download the damn file
				cachaca( j , tf , mode = 'wb' )
				
				# unzip the damn file
				unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )
				
				# copy all unzipped files into the year-appropriate directory
				stopifnot( all( file.copy( unzipped_files , paste0( catalog[ i , 'output_folder' ] , "/" , tolower( basename( unzipped_files ) ) ) ) ) )
						
			}


			# # # # # # # # # # #
			# import all files  #
			# # # # # # # # # # #
			
			# construct a vector with all downloaded files
			files <- list.files( catalog[ i , 'output_folder' ] , full.names = TRUE )
			
			# figure out the unique three-character prefixes of each file
			prefixes <- unique( substr( basename( files ) , 1 , 3 ) )

			# loop through each prefix
			for ( p in prefixes ){
			
				# initiate an empty object
				y <- NULL
				
				# loop through each saved file matching the prefix pattern
				for ( this.file in files[ substr( basename( files ) , 1 , 3 ) == p ] ){
				
					# read the file into RAM
					x <- haven::read_spss( this.file )
					
					# coerce the file into a data.frame object
					x <- as.data.frame( x )
					
					# stack it
					y <- rbind( y , x )
					
					# remove the original file from the disk
					file.remove( this.file )
					
				}
				
				# make all column names lowercase
				names( y ) <- tolower( names( y ) )
				
				# save the stacked file as the prefix
				assign( p , y )
				
				# save that single all-country stack-a-mole
				save( list = p , file = paste0( catalog[ i , 'output_folder' ] , '/' , p , '.rda' ) )
				
			}


			# here are the combinations to create #
	
			# asa alone #
			asa_name <- load( paste0( catalog[ i , 'output_folder' ] , '/asa.rda' ) )
			asa <- get( asa_name )
			asa_design <- pirls_pvsd( asa , 'totwgt' , pirls_rwcf( asa , 'totwgt' ) )
			save( asa_design , file = paste0( catalog[ i , 'output_folder' ] , '/asa_design.rda' ) )
			
			# asg alone #
			asg_name <- load( paste0( catalog[ i , 'output_folder' ] , '/asg.rda' ) )
			asg <- get( asg_name )
			asg_design <- pirls_pvsd( asg , 'totwgt' , pirls_rwcf( asg , 'totwgt' ) )
			save( asg_design , file = paste0( catalog[ i , 'output_folder' ] , '/asg_design.rda' ) )
			
			# asg + ash
			ash_name <- load( paste0( catalog[ i , 'output_folder' ] , '/ash.rda' ) )
			ash <- get( ash_name )
			# note: these two files are too big to merge in a smaller computer's RAM
			# so only keep the weighting/jackknife variables from the `asg` table for this iteration.
			asg_ash <- merge( asg[ , c( 'idcntry' , 'idstud' , 'totwgt' , 'jkzone' , 'jkrep' ) ] , ash , by = c( 'idcntry' , 'idstud' ) )
			stopifnot( nrow( asg_ash ) == nrow( ash ) & nrow( ash ) == nrow( asg ) )

			asg_ash_design <- pirls_pvsd( asg_ash , 'totwgt' , pirls_rwcf( asg_ash , 'totwgt' ) )
			save( asg_ash_design , file = paste0( catalog[ i , 'output_folder' ] , '/asg_ash_design.rda' ) )

			
			# asg + acg
			acg_name <- load( paste0( catalog[ i , 'output_folder' ] , '/acg.rda' ) )
			acg <- get( acg_name )
			asg_acg <- merge( asg , acg , by = c( 'idcntry' , 'idschool' ) )
			stopifnot( nrow( asg_acg ) == nrow( asg ) )
			
			asg_acg_design <- pirls_pvsd( asg_acg , 'totwgt' , pirls_rwcf( asg_acg , 'totwgt' ) )
			save( asg_acg_design , file = paste0( catalog[ i , 'output_folder' ] , '/asg_acg_design.rda' ) )

			
			# ast alone #
			ast_name <- load( paste0( catalog[ i , 'output_folder' ] , '/ast.rda' ) )
			ast <- get( ast_name )
			ast_design <- pirls_pvsd( ast , 'tchwgt' , pirls_rwcf( ast , 'tchwgt' ) )
			save( ast_design , file = paste0( catalog[ i , 'output_folder' ] , '/ast_design.rda' ) )

			
			# ast + atg
			atg_name <- load( paste0( catalog[ i , 'output_folder' ] , '/atg.rda' ) )
			atg <- get( atg_name )
			ast_atg <- merge( ast , atg , by = c( 'idcntry' , 'idteach' , 'idlink' ) )
			stopifnot( nrow( ast_atg ) == nrow( ast ) )

			ast_atg_design <- pirls_pvsd( ast_atg , 'tchwgt' , pirls_rwcf( ast_atg , 'tchwgt' ) )
			save( ast_atg_design , file = paste0( catalog[ i , 'output_folder' ] , '/ast_atg_design.rda' ) )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}



# initiate a replicate weights creation function to quickly compute weights wherever necessary
pirls_rwcf <- 
	function( z , wgt ){
		for ( i in 1:75 ){
			z[ z$jkzone != i , paste0( 'rw' , i ) ] <- z[ z$jkzone != i , wgt ]
			z[ z$jkzone == i & z$jkrep == 1 , paste0( 'rw' , i ) ] <- z[ z$jkzone == i & z$jkrep == 1 , wgt ] * 2
			z[ z$jkzone == i & z$jkrep == 0 , paste0( 'rw' , i ) ] <- 0
		}
		z[ , paste0( 'rw' , 1:75 ) ]
}


# initiate a plausible valued survey design object creation function
pirls_pvsd <-
	function( x , wgt , rw ){
	
		# search for plausible values variables.
		# plausible values variables are named _varname_01 thru _varname_05
		ppv <- grep( "(.*)0[1-5]$" , names( x ) , value = TRUE )
		
		ppv <- unique( gsub( "0[1-5]$" , "" , ppv ) )
		
		# actual plausible values
		pv <- NULL
		
		# confirm '01' thru '05' are in the data set.
		for ( i in ppv ) if ( all( paste0( i , '0' , 1:5 ) %in% names( x ) ) ) pv <- c( pv , i )
		
		
		# if there are any plausible values variables,
		# the survey design needs to be both multiply-imputed and replicate-weighted.
		if( length( pv ) > 0 ){
		
			# loop through all five iterations of the plausible value
			for ( i in 1:5 ){
					
				y <- x
		
				# loop through each plausible value variable
				for ( vn in pv ){
				
					# copy over the correct iteration into the main variable
					y[ , vn ] <- y[ , paste0( vn , '0' , i ) ]
					
					# erase all five originals
					y <- y[ , !( names( y ) %in% paste0( vn , '0' , 1:5 ) ) ]

				}
				
				# save the implicate
				assign( paste0( 'x' , i ) , y )
			}
			
			z <-
				survey::svrepdesign( 	
					weights = as.formula( paste( "~" , wgt ) ) , 
					repweights = rw ,
					rscales = rep( 1 , 75 ) ,
					scale = 1 ,
					type = 'other' ,
					data = mitools::imputationList( mget( paste0( "x" , 1:5 ) ) ) ,
					mse = TRUE
				)
				
		# otherwise, it's simply replicate-weighted.
		} else {
		
			z <-
				survey::svrepdesign( 	
					weights = as.formula( paste( "~" , wgt ) ) , 
					repweights = rw ,
					rscales = rep( 1 , 75 ) ,
					scale = 1 ,
					type = 'other' ,
					data = x ,
					mse = TRUE
				)
				
		}
	
		z
	}



