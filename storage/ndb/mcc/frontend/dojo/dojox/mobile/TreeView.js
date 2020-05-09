define([
	"dojo/_base/kernel",
	"dojo/_base/array",
	"dojo/_base/declare",
	"dojo/_base/lang",
	"dojo/_base/window",
	"dojo/dom-construct",
	"dijit/registry",
	"./Heading",
	"./ListItem",
	"./ProgressIndicator",
	"./RoundRectList",
	"./ScrollableView",
	"./viewRegistry",
	"dojo/has",
	"dojo/has!dojo-bidi?dojox/mobile/bidi/TreeView"
], function(kernel, array, declare, lang, win, domConstruct, registry, Heading, ListItem, ProgressIndicator, RoundRectList, ScrollableView, viewRegistry, has, BidiTreeView){

	// module:
	//		dojox/mobile/TreeView

	kernel.experimental("dojox.mobile.TreeView");

	var TreeView = declare(has("dojo-bidi") ? "dojox.mobile.NonBidiTreeView" : "dojox.mobile.TreeView", ScrollableView, {
		// summary:
		//		A scrollable view with tree-style navigation.
		// description:
		//		This widget can be connected to a dojox/data/FileStore as a
		//		quick directory browser. You may use it when implementing the
		//		Master-Detail pattern.

		postCreate: function(){
			this._load();
			this.inherited(arguments);
		},
		
		_customizeListItem: function(listItemArgs){
		},

		_load: function(){
			this.model.getRoot(
				lang.hitch(this, function(item){
					var scope = this;
					var list = new RoundRectList();
					var node = {};
					var listItemArgs = {
						label: scope.model.rootLabel,
						moveTo: '#',
						onClick: function(){ scope.handleClick(this); },
						item: item
					};
					this._customizeListItem(listItemArgs);
					var listitem = new ListItem(listItemArgs);
					list.addChild(listitem);
					this.addChild(list);
				})
			)
		},

		handleClick: function(li){
			// summary:
			//		Called when the user clicks a tree item.
			// li: dojox/mobile/ListItem
			//		The item that the user clicked.
			var newViewId = "view_";
			if(li.item[this.model.newItemIdAttr]){
				newViewId += li.item[this.model.newItemIdAttr];
			}else{
				newViewId += "rootView";
			}
			newViewId = newViewId.replace('/', '_');
			if(registry.byId(newViewId)){  // view already exists, just transition to it
				registry.byNode(li.domNode).transitionTo(newViewId);
				return;
			}

			var prog = ProgressIndicator.getInstance();
			win.body().appendChild(prog.domNode);
			prog.start();

			this.model.getChildren(li.item,
				lang.hitch(this, function(items){
					var scope = this;
					var list = new RoundRectList();
					array.forEach(items, function(item, i){
						var listItemArgs = {
							item: item,
							label: item[scope.model.store.label],
							transition: "slide"
						};
						scope._customizeListItem(listItemArgs);
						if(scope.model.mayHaveChildren(item)){
							listItemArgs.moveTo = '#';
							listItemArgs.onClick = function(){ scope.handleClick(this); };
						}
						var listitem = new ListItem(listItemArgs);
						list.addChild(listitem);
					});

					var heading = new Heading({
						label: "Dynamic View",
						back: "Back",
						moveTo: viewRegistry.getEnclosingView(li.domNode).id,
						dir: this.isLeftToRight() ? "ltr" : "rtl"
					});

					var newView = ScrollableView({
						id: newViewId,
						dir: this.isLeftToRight() ? "ltr" : "rtl"
					}, domConstruct.create("div", null, win.body()));
					newView.addChild(heading);
					newView.addChild(list);
					newView.startup();
					prog.stop();
					registry.byNode(li.domNode).transitionTo(newView.id);
				})
			)
		}
	});
	
	return has("dojo-bidi") ? declare("dojox.mobile.TreeView", [TreeView, BidiTreeView]) : TreeView;		
});
