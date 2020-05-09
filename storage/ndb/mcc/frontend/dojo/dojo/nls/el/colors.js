define(
({
// local representation of all CSS3 named colors, companion to dojo.colors.  To be used where descriptive information
// is required for each color, such as a palette widget, and not for specifying color programatically.
	//Note: due to the SVG 1.0 spec additions, some of these are alternate spellings for the same color (e.g. gray / grey).
	//TODO: should we be using unique rgb values as keys instead and avoid these duplicates, or rely on the caller to do the reverse mapping?
	aliceblue: "σιέλ",
	antiquewhite: "ξεθωριασμένο λευκό",
	aqua: "γαλάζιο",
	aquamarine: "γαλαζοπράσινο",
	azure: "μπλε του ουρανού",
	beige: "μπεζ",
	bisque: "σκούρο κρεμ",
	black: "μαύρο",
	blanchedalmond: "ζαχαρί",
	blue: "μπλε",
	blueviolet: "βιολετί",
	brown: "καφέ",
	burlywood: "καφέ του ξύλου",
	cadetblue: "μπλε του στρατού",
	chartreuse: "φωτεινό κιτρινοπράσινο",
	chocolate: "σοκολατί",
	coral: "κοραλί",
	cornflowerblue: "μεσαίο μπλε",
	cornsilk: "ασημί του καλαμποκιού",
	crimson: "βαθύ κόκκινο",
	cyan: "κυανό",
	darkblue: "σκούρο μπλε",
	darkcyan: "σκούρο κυανό",
	darkgoldenrod: "σκούρο χρυσοκίτρινο",
	darkgray: "σκούρο γκρι",
	darkgreen: "σκούρο πράσινο",
	darkgrey: "σκούρο γκρι", // same as darkgray
	darkkhaki: "σκούρο χακί",
	darkmagenta: "σκούρο ματζέντα",
	darkolivegreen: "σκούρο πράσινο λαδί",
	darkorange: "σκούρο πορτοκαλί",
	darkorchid: "σκούρα ορχιδέα",
	darkred: "σκούρο κόκκινο",
	darksalmon: "σκούρο σομόν",
	darkseagreen: "σκούρο πράσινο της θάλασσας",
	darkslateblue: "σκούρο μεταλλικό μπλε",
	darkslategray: "σκούρο μεταλλικό γκρι",
	darkslategrey: "σκούρο μεταλλικό γκρι", // same as darkslategray
	darkturquoise: "σκούρο τυρκουάζ",
	darkviolet: "σκούρο βιολετί",
	deeppink: "βαθύ ροζ",
	deepskyblue: "βαθύ μπλε το ουρανού",
	dimgray: "αχνό γκρι",
	dimgrey: "αχνό γκρι", // same as dimgray
	dodgerblue: "σκούρο ελεκτρίκ",
	firebrick: "κεραμιδί",
	floralwhite: "λευκό των ανθών",
	forestgreen: "πράσινο του δάσους",
	fuchsia: "φούξια",
	gainsboro: "γκρι σιέλ",
	ghostwhite: "άσπρο",
	gold: "χρυσαφί",
	goldenrod: "χρυσοκίτρινο",
	gray: "γκρι",
	green: "πράσινο",
	greenyellow: "πρασινοκίτρινο",
	grey: "γκρι", // same as gray
	honeydew: "μελί",
	hotpink: "έντονο ροζ",
	indianred: "ινδικό κόκκινο",
	indigo: "λουλακί",
	ivory: "ιβουάρ",
	khaki: "χακί",
	lavender: "λίλα",
	lavenderblush: "μωβ λεβάντας",
	lawngreen: "σκούρο πράσινο",
	lemonchiffon: "λεμονί",
	lightblue: "ανοιχτό μπλε",
	lightcoral: "ανοιχτό κοραλί",
	lightcyan: "ανοιχτό κυανό",
	lightgoldenrodyellow: "ανοιχτό χρυσοκίτρινο",
	lightgray: "ανοιχτό γκρι",
	lightgreen: "ανοιχτό πράσινο",
	lightgrey: "ανοιχτό γκρι", // same as lightgray
	lightpink: "ανοιχτό ροζ",
	lightsalmon: "ανοιχτό σομόν",
	lightseagreen: "ανοιχτό πράσινο της θάλασσας",
	lightskyblue: "ανοιχτό μπλε το ουρανού",
	lightslategray: "ανοιχτό μεταλλικό γκρι",
	lightslategrey: "ανοιχτό μεταλλικό γκρι", // same as lightslategray
	lightsteelblue: "ανοιχτό μπλε ατσαλιού",
	lightyellow: "ανοιχτό κίτρινο",
	lime: "λαχανί",
	limegreen: "πράσινο λαχανί",
	linen: "σπαγγί",
	magenta: "ματζέντα",
	maroon: "βυσσινί",
	mediumaquamarine: "μεσαίο γαλαζοπράσινο",
	mediumblue: "μεσαίο μπλε",
	mediumorchid: "μεσαία ορχιδέα",
	mediumpurple: "μεσαίο μωβ",
	mediumseagreen: "μεσαίο πράσινο της θάλασσας",
	mediumslateblue: "μεσαίο μεταλλικό μπλε",
	mediumspringgreen: "μεσαίο πράσινο της άνοιξης",
	mediumturquoise: "μεσαίο τυρκουάζ",
	mediumvioletred: "μεσαίο κόκκινο βιολετί",
	midnightblue: "πολύ σκούρο μπλε",
	mintcream: "βεραμάν",
	mistyrose: "τριανταφυλλί",
	moccasin: "μόκα",
	navajowhite: "άσπρο Ναβάχο",
	navy: "μπλε του ναυτικού",
	oldlace: "εκρού",
	olive: "πράσινο λαδί",
	olivedrab: "λαδί",
	orange: "πορτοκαλί",
	orangered: "πορτοκαλοκόκκινο",
	orchid: "ορχιδέα",
	palegoldenrod: "αχνό χρυσοκίτρινο",
	palegreen: "αχνό πράσινο",
	paleturquoise: "αχνό τυρκουάζ",
	palevioletred: "αχνό κόκκινο βιολετί",
	papayawhip: "αχνό ροζ",
	peachpuff: "ροδακινί",
	peru: "περού",
	pink: "ροζ",
	plum: "δαμασκηνί",
	powderblue: "αχνό μπλε",
	purple: "μωβ",
	red: "κόκκινο",
	rosybrown: "καστανό",
	royalblue: "έντονο μπλε",
	saddlebrown: "βαθύ καφέ",
	salmon: "σομόν",
	sandybrown: "μπεζ της άμμου",
	seagreen: "πράσινο της θάλασσας",
	seashell: "κοχύλι",
	sienna: "καφεκίτρινο",
	silver: "ασημί",
	skyblue: "μπλε του ουρανού",
	slateblue: "μεταλλικό μπλε",
	slategray: "μεταλλικό γκρι",
	slategrey: "μεταλλικό γκρι", // same as slategray
	snow: "χιονί",
	springgreen: "πράσινο της άνοιξης",
	steelblue: "μπλε ατσαλιού",
	tan: "ώχρα",
	teal: "πετρόλ",
	thistle: "μωβ βιολετί",
	tomato: "κόκκινο της ντομάτας",
	transparent: "διαφανές",
	turquoise: "τυρκουάζ",
	violet: "βιολετί",
	wheat: "σταρένιο",
	white: "λευκό",
	whitesmoke: "λευκός καπνός",
	yellow: "κίτρινο",
	yellowgreen: "κιτρινοπράσινο"
})
);
