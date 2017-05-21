
	
# svyttest() variant (code from the `survey` package)
# that works on multiply-imputed data
MIsvyttest<-function(formula, design ,...){

	# the MIcombine function runs differently than a normal svyglm() call
	m <- eval(bquote(mitools::MIcombine( with( design , survey::svyglm(formula,family=gaussian()))) ) )

	rval<-list(statistic=coef(m)[2]/survey::SE(m)[2],
			   parameter=m$df[2],		
			   estimate=coef(m)[2],
			   null.value=0,
			   alternative="two.sided",
			   method="Design-based t-test",
			   data.name=deparse(formula))
			   
	rval$p.value <- ( 1 - pf( ( rval$statistic )^2 , 1 , m$df[2] ) )

	names(rval$statistic)<-"t"
	names(rval$parameter)<-"df"
	names(rval$estimate)<-"difference in mean"
	names(rval$null.value)<-"difference in mean"
	class(rval)<-"htest"

	return(rval)
  
}



MIsvyciprop <-
	function (formula, design, method = c("logit", "likelihood", 
		"asin", "beta", "mean", "xlogit"), level = 0.95, df = mean(unlist(lapply(design$designs,survey::degf))), 
		...) 
	{
		method <- match.arg(method)
		if (method == "mean") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])),...)))))
			ci <- as.vector(confint(m, 1, level = level, df = df, 
				...))
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "asin") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			m <- structure(coef(m), .Names = "1", var = m$variance[1], .Dim = c(1L, 1L), statistic = "mean", class = "svystat")
			xform <- survey::svycontrast(m, quote(asin(sqrt(`1`))))
			ci <- sin(as.vector(confint(xform, 1, level = level, 
				df = df, ...)))^2
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "xlogit") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			m <- structure(coef(m), .Names = "1", var = m$variance[1], .Dim = c(1L, 1L), statistic = "mean", class = "svystat")
			xform <- survey::svycontrast(m, quote(log(`1`/(1 - `1`))))
			ci <- survey:::expit(as.vector(confint(xform, 1, level = level, 
				df = df, ...)))
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "beta") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			n.eff <- coef(m) * (1 - coef(m))/vcov(m)
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
			alpha <- 1 - level
			n.eff <- n.eff * (qt(alpha/2, mean(unlist(lapply(design$designs,nrow))) - 1)/qt(alpha/2, 
				mean(unlist(lapply(design$designs,survey::degf)))))^2
			ci <- c(qbeta(alpha/2, n.eff * rval, n.eff * (1 - rval) + 
				1), qbeta(1 - alpha/2, n.eff * rval + 1, n.eff * 
				(1 - rval)))
		}
		else {
			m <- eval(bquote(mitools::MIcombine(with(design,svyglm(.(formula[[2]]) ~ 1, family = quasibinomial)))))
			cimethod <- switch(method, logit = "Wald", likelihood = "likelihood")
			ci <- suppressMessages(as.numeric(survey:::expit(confint(m, 1, 
				level = level, method = cimethod, ddf = df))))
			rval <- survey:::expit(coef(m))[1]
			attr(rval, "var") <- vcov(eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...))))))
		}
		halfalpha <- (1 - level)/2
		names(ci) <- paste(round(c(halfalpha, (1 - halfalpha)) * 
			100, 1), "%", sep = "")
		names(rval) <- deparse(formula[[2]])
		attr(rval, "ci") <- ci
		class(rval) <- "svyciprop"
		rval
	}
	
	
# pulled from the R survey library
expit <- function(eta) exp(eta)/(1 + exp(eta))
