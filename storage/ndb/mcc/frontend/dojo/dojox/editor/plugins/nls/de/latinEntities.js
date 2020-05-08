define(
({
	/* These are already handled in the default RTE
	amp:"ampersand",lt:"less-than sign",
	gt:"greater-than sign",
	nbsp:"no-break space\nnon-breaking space",
	quot:"quote",
	*/
	iexcl:"Umgekehrtes Ausrufezeichen",
	cent:"Cent-Zeichen",
	pound:"Nummernzeichen",
	curren:"Währungssymbol",
	yen:"Yen-Zeichen\nYuan-Zeichen",
	brvbar:"Unterbrochener Balken\nUnterbrochener vertikaler Balken",
	sect:"Abschnittszeichen",
	uml:"Trema\nPünktchen oben",
	copy:"Copyrightzeichen",
	ordf:"Weibliches Ordinalzeichen",
	laquo:"Doppelte, winklige Anführungszeichen, die nach links weisen\linke französische Anführungszeichen",
	not:"Nicht-Zeichen",
	shy:"Veränderlicher Silbentrennstrich\nWeicher Bindestrich",
	reg:"Registrierte Handelsmarke\nRegistriertes Markenzeichen",
	macr:"Makron\nLängestrich\nÜberstrich\nKurzer Überstrich über einem Buchstaben",
	deg:"Gradzeichen",
	plusmn:"Plus-Minus-Zeichen\nPlus-oder-Minus-Zeichen",
	sup2:"Hochgestellte Zwei\nHoch 2\nZum Quadrat",
	sup3:"Hochgestellte Drei\nHoch 3\nKubik",
	acute:"Akut\nAkutzeichen",
	micro:"Micro-Zeichen",
	para:"Pilcrow-Zeichen\nAbsatzzeichen",
	middot:"Multiplikationszeichen\nGeorgisches Komma\nGriechisches Multiplikationszeichen",
	cedil:"Cedille\nC mit Häkchen",
	sup1:"Hochgestellte Eins\nHoch 1",
	ordm:"Männliches Ordinalzeichen",
	raquo:"Doppelte, winklige Anführungszeichen, die nach rechts weisen\nRechtes französisches Anführungszeichen",
	frac14:"Bruch 1 durch 4\nEin Viertel",
	frac12:"Bruch 1 durch 2\nEinhalb",
	frac34:"Bruch 3 durch 4\nDreiviertel",
	iquest:"Umgekehrtes Fragezeichen\nFragezeichen auf dem Kopf",
	Agrave:"Großes A mit Gravis\nGroßbuchstabe A mit Gravis",
	Aacute:"Großbuchstabe A mit Akut",
	Acirc:"Großbuchstabe A mit Zirkumflex",
	Atilde:"Großbuchstabe A mit Tilde",
	Auml:"Großbuchstabe A mit Trema",
	Aring:"Großes A mit Ring darüber\nLateinischer Großbuchstabe A mit Ring darüber",
	AElig:"Großes AE\nLigatur aus Großbuchstaben A und E",
	Ccedil:"Großbuchstabe C mit Cedilla",
	Egrave:"Großbuchstabe E mit Gravis",
	Eacute:"Großbuchstabe E mit Akut",
	Ecirc:"Großbuchstabe E mit Zirkumflex",
	Euml:"Großbuchstabe E mit Trema",
	Igrave:"Großbuchstabe I mit Gravis",
	Iacute:"Großbuchstabe I mit Akut",
	Icirc:"Großbuchstabe I mit Zirkumflex",
	Iuml:"Großbuchstabe I mit Trema",
	ETH:"Großes ETH",
	Ntilde:"Großbuchstabe N mit Tilde",
	Ograve:"Großbuchstabe O mit Gravis",
	Oacute:"Großbuchstabe O mit Akut",
	Ocirc:"Großbuchstabe O mit Zirkumflex",
	Otilde:"Lateinischer Großbuchstabe O mit Tilde",
	Ouml:"Lateinischer Großbuchstabe O mit Trema",
	times:"Multiplikationszeichen",
	Oslash:"Großes O mit Schrägstrich\nGroßer dänisch-norwegischer Umlaut ö",
	Ugrave:"Großbuchstabe U mit Gravis",
	Uacute:"Großbuchstabe U mit Akut",
	Ucirc:"Großbuchstabe U mit Zirkumflex",
	Uuml:"Großbuchstabe U mit Trema",
	Yacute:"Großbuchstabe Y mit Akut",
	THORN:"Großes THORN",
	szlig:"Scharfes s\nEsszett",
	agrave:"Kleines a mit Gravis\nKleinbuchstabe a mit Gravis",
	aacute:"Kleinbuchstabe a mit Aktut",
	acirc:"Kleinbuchstabe a mit Zirkumflex",
	atilde:"Kleinbuchstabe a mit Tilde",
	auml:"Kleinbuchstabe a mit Trema",
	aring:"Kleines a mit Ring darüber\nKleinbuchstabe a mit Ring",
	aelig:"Kleines ae\nLigatur aus Kleinbuchstaben a und e",
	ccedil:"Kleinbuchstabe c mit Cedilla",
	egrave:"Kleinbuchstabe e mit Gravis",
	eacute:"Kleinbuchstabe e mit Aktut",
	ecirc:"Kleinbuchstabe e mit Zirkumflex",
	euml:"Kleinbuchstabe e mit Trema",
	igrave:"Kleinbuchstabe i mit Gravis",
	iacute:"Kleinbuchstabe i mit Aktut",
	icirc:"Kleinbuchstabe i mit Zirkumflex",
	iuml:"Kleinbuchstabe i mit Trema",
	eth:"Kleines eth",
	ntilde:"Kleinbuchstabe n mit Tilde",
	ograve:"Kleinbuchstabe o mit Gravis",
	oacute:"Kleinbuchstabe o mit Aktut",
	ocirc:"Kleinbuchstabe o mit Zirkumflex",
	otilde:"Kleinbuchstabe o mit Tilde",
	ouml:"Kleinbuchstabe o mit Gravis",
	divide:"Divisionszeichen",
	oslash:"Kleines o mit Schrägstrich\nKleiner dänisch-norwegischer Umlaut ö",
	ugrave:"Kleinbuchstabe u mit Gravis",
	uacute:"Kleinbuchstabe u mit Aktut",
	ucirc:"Kleinbuchstabe u mit Zirkumflex",
	uuml:"Kleinbuchstabe u mit Trema",
	yacute:"Kleinbuchstabe y mit Aktut",
	thorn:"Kleines thorn",
	yuml:"Kleinbuchstabe y mit Trema",
// Greek Characters and Symbols
	fnof:"Kleines f mit Haken\nFunction\nFlorin",
	Alpha:"Griechischer Großbuchstabe Alpha",
	Beta:"Griechischer Großbuchstabe Beta",
	Gamma:"Griechischer Großbuchstabe Gamma",
	Delta:"Griechischer Großbuchstabe Delta",
	Epsilon:"Griechischer Großbuchstabe Epsilon",
	Zeta:"Griechischer Großbuchstabe Zeta",
	Eta:"Griechischer Großbuchstabe Eta",
	Theta:"Griechischer Großbuchstabe Theta",
	Iota:"Griechischer Großbuchstabe Iota",
	Kappa:"Griechischer Großbuchstabe Kappa",
	Lambda:"Griechischer Großbuchstabe Lambda",
	Mu:"Griechischer Großbuchstabe My",
	Nu:"Griechischer Großbuchstabe Ny",
	Xi:"Griechischer Großbuchstabe Xi",
	Omicron:"Griechischer Großbuchstabe Omicron",
	Pi:"Griechischer Großbuchstabe Pi",
	Rho:"Griechischer Großbuchstabe Rho",
	Sigma:"Griechischer Großbuchstabe Sigma",
	Tau:"Griechischer Großbuchstabe Tau",
	Upsilon:"Griechischer Großbuchstabe Upsilon",
	Phi:"Griechischer Großbuchstabe Phi",
	Chi:"Griechischer Großbuchstabe Chi",
	Psi:"Griechischer Großbuchstabe Psi",
	Omega:"Griechischer Großbuchstabe Omega",
	alpha:"Griechischer Kleinbuchstabe Alpha",
	beta:"Griechischer Kleinbuchstabe Beta",
	gamma:"Griechischer Kleinbuchstabe Gamma",
	delta:"Griechischer Kleinbuchstabe Delta",
	epsilon:"Griechischer Kleinbuchstabe Epsilon",
	zeta:"Griechischer Kleinbuchstabe Zeta",
	eta:"Griechischer Kleinbuchstabe Eta",
	theta:"Griechischer Kleinbuchstabe Theta",
	iota:"Griechischer Kleinbuchstabe Iota",
	kappa:"Griechischer Kleinbuchstabe Kappa",
	lambda:"Griechischer Kleinbuchstabe Lambda",
	mu:"Griechischer Kleinbuchstabe My",
	nu:"Griechischer Kleinbuchstabe Ny",
	xi:"Griechischer Kleinbuchstabe Xi",
	omicron:"Griechischer Kleinbuchstabe Omicron",
	pi:"Griechischer Kleinbuchstabe Pi",
	rho:"Griechischer Kleinbuchstabe Rho",
	sigmaf:"Griechischer Kleinbuchstabe Sigma am Wortende",
	sigma:"Griechischer Kleinbuchstabe Sigma",
	tau:"Griechischer Kleinbuchstabe Tau",
	upsilon:"Griechischer Kleinbuchstabe Upsilon",
	phi:"Griechischer Kleinbuchstabe Phi",
	chi:"Griechischer Kleinbuchstabe Chi",
	psi:"Griechischer Kleinbuchstabe Psi",
	omega:"Griechischer Kleinbuchstabe Omega",
	thetasym:"Griechischer Kleinbuchstabe Theta (Symbol)",
	upsih:"Griechisches Upsilon mit Haken",
	piv:"Griechisches Pi-Symbol",
	bull:"Rundes Aufzählungszeichen\nSchwarzer kleiner Kreis",
	hellip:"Auslassung\nDrei kleine Punkte",
	prime:"Prime\nMinuten\nFuß",
	Prime:"Doppelstrich\nSekunden\nZoll",
	oline:"Überstrich\nÜberstreichungszeichen",
	frasl:"Schrägstrich für Bruch",
	weierp:"Kleines p in Schreibschrift\nPotenz\nWeierstraß-p",
	image:"Großes I in Frakturschrift\nImaginärteil",
	real:"Großes R in Frakturschrift\nRealteilsymbol",
	trade:"Markenzeichen",
	alefsym:"Aleph-Symbol\nErste transfinite Kardinalzahl",
	larr:"Linkspfeil",
	uarr:"Aufwärtspfeil",
	rarr:"Rechtspfeil",
	darr:"Abwärtspfeil",
	harr:"Links-Rechts-Pfeil",
	crarr:"Abwärtspfeil, der nach links abknickt\nZeilenumbruch",
	lArr:"Doppelter Linkspfeil",
	uArr:"Doppelter Aufwärtspfeil",
	rArr:"Doppelter Rechtspfeil",
	dArr:"Doppelter Abwärtspfeil",
	hArr:"Doppelter Rechts-Links-Pfeil",
	forall:"Für alle",
	part:"Partielle Differenzialgleichung",
	exist:"Es existiert",
	empty:"Leere Menge\nNullmenge\nDurchmesser",
	nabla:"Nabla\nRückwärtsdifferenz",
	isin:"Element von",
	notin:"Kein Element von",
	ni:"Enthält als Member",
	prod:"Unäres Produkt\nProduktzeichen",
	sum:"Unäre Summation",
	minus:"Minuszeichen",
	lowast:"Sternoperator",
	radic:"Quadratwurzel\nWurzelzeichen",
	prop:"proportional zu",
	infin:"Unendlich",
	ang:"Winkel",
	and:"Logisches UND\nHatschek",
	or:"Logisches ODER\nV",
	cap:"Schnittmenge\nBogen mit Öffnung oben",
	cup:"Vereinigungsmenge\nBogen mit Öffnung unten","int":"Integral",
	there4:"Deshalb",
	sim:"Tildenoperator\nVariiert mit\nÄhnlich wie",
	cong:"Etwa gleich mit",
	asymp:"Ungefähr gleich mit\nAsymptotisch",
	ne:"Nicht gleich mit",
	equiv:"Identisch mit",
	le:"Kleiner-gleich",
	ge:"Größer-gleich",
	sub:"Teil von",
	sup:"Obermenge von",
	nsub:"Kein Teil von",
	sube:"Teilmenge oder gleich mit",
	supe:"Obermenge oder gleich mit",
	oplus:"In Kreis eingeschlossenes Pluszeichen\nDirekte Summe",
	otimes:"In Kreis eingeschlossenes X\nVektorprodukt",
	perp:"Senkrecht\nOrthogonal zu\nPerpendikulär",
	sdot:"Punktoperator",
	lceil:"Linke Ecke oben\nSenkrechter Winkel mit Ecke links oben",
	rceil:"Rechte Ecke oben",
	lfloor:"Linke Ecke unten\nSenkrechter Winkel mit Ecke links unten",
	rfloor:"Rechte Ecke unten",
	lang:"Linke spitze Klammer",
	rang:"Rechte spitze Klammer",
	loz:"Raute",
	spades:"Schwarzes Pik (Kartenspiel)",
	clubs:"Schwarzes Kreuz (Kartenspiel)\nKleeblatt",
	hearts:"Schwarzes Herz (Kartenspiel)\nValentine",
	diams:"Schwarzes Karo (Kartenspiel)",
	OElig:"Ligatur aus Großbuchstaben O und E",
	oelig:"Ligatur aus Kleinbuchstaben o und e",
	Scaron:"Großbuchstabe S mit Caron",
	scaron:"Kleinbuchstabe s mit Caron",
	Yuml:"Großbuchstabe Y mit Trema",
	circ:"Zirkumflex, Akzent",
	tilde:"kleine Tilde",
	ensp:"Leerschritt von der Breite des Buchstaben n",
	emsp:"Leerschritt von der Breite des Buchstaben m",
	thinsp:"Schmaler Leerschritt",
	zwnj:"Nichtverbinder mit Nullbreite",
	zwj:"Verbinder mit Nullbreite",
	lrm:"Links-Rechts-Markierung",
	rlm:"Rechts-Links-Markierung",
	ndash:"Gedankenstrich von der Länge des Buchstabens n",
	mdash:"Gedankenstrich von der Länge des Buchstabens m",
	lsquo:"Linkes einfaches Anführungszeichen",
	rsquo:"Rechtes einfaches Anführungszeichen",
	sbquo:"Einfaches, gekrümmtes Anführungszeichen unten",
	ldquo:"Linkes doppeltes Anführungszeichen",
	rdquo:"Rechtes doppeltes Anführungszeichen",
	bdquo:"Doppeltes, gekrümmtes Anführungszeichen unten",
	dagger:"Kreuzzeichen",
	Dagger:"Doppelkreuzzeichen",
	permil:"Promillezeichen",
	lsaquo:"Einfaches linkes Anführungszeichen",
	rsaquo:"Einfaches rechtes Anführungszeichen",
	euro:"Euro-Zeichen"
})
);
