//>>built
define(["dijit","dojo","dojox","dojo/require!dojox/drawing/manager/_registry,dojox/gfx,dojox/drawing/Drawing,dojox/drawing/util/oo,dojox/drawing/util/common,dojox/drawing/util/typeset,dojox/drawing/defaults,dojox/drawing/manager/Canvas,dojox/drawing/manager/Undo,dojox/drawing/manager/keys,dojox/drawing/manager/Mouse,dojox/drawing/manager/Stencil,dojox/drawing/manager/StencilUI,dojox/drawing/manager/Anchors,dojox/drawing/stencil/_Base,dojox/drawing/stencil/Line,dojox/drawing/stencil/Rect,dojox/drawing/stencil/Ellipse,dojox/drawing/stencil/Path,dojox/drawing/stencil/Text,dojox/drawing/stencil/Image,dojox/drawing/annotations/Label,dojox/drawing/annotations/Angle,dojox/drawing/annotations/Arrow,dojox/drawing/annotations/BoxShadow"],function(_1,_2,_3){
_2.provide("dojox.drawing._base");
_2.experimental("dojox.drawing");
_2.require("dojox.drawing.manager._registry");
_2.require("dojox.gfx");
_2.require("dojox.drawing.Drawing");
_2.require("dojox.drawing.util.oo");
_2.require("dojox.drawing.util.common");
_2.require("dojox.drawing.util.typeset");
_2.require("dojox.drawing.defaults");
_2.require("dojox.drawing.manager.Canvas");
_2.require("dojox.drawing.manager.Undo");
_2.require("dojox.drawing.manager.keys");
_2.require("dojox.drawing.manager.Mouse");
_2.require("dojox.drawing.manager.Stencil");
_2.require("dojox.drawing.manager.StencilUI");
_2.require("dojox.drawing.manager.Anchors");
_2.require("dojox.drawing.stencil._Base");
_2.require("dojox.drawing.stencil.Line");
_2.require("dojox.drawing.stencil.Rect");
_2.require("dojox.drawing.stencil.Ellipse");
_2.require("dojox.drawing.stencil.Path");
_2.require("dojox.drawing.stencil.Text");
_2.require("dojox.drawing.stencil.Image");
_2.require("dojox.drawing.annotations.Label");
_2.require("dojox.drawing.annotations.Angle");
_2.require("dojox.drawing.annotations.Arrow");
_2.require("dojox.drawing.annotations.BoxShadow");
});
