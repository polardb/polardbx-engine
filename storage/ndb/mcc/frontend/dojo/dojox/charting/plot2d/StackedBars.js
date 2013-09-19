//>>built
define("dojox/charting/plot2d/StackedBars",["dojo/_base/lang","dojo/_base/array","dojo/_base/declare","./Bars","./common","dojox/lang/functional","dojox/lang/functional/reversed","dojox/lang/functional/sequence"],function(_1,_2,_3,_4,dc,df,_5,_6){
var _7=_5.lambda("item.purgeGroup()");
return _3("dojox.charting.plot2d.StackedBars",_4,{getSeriesStats:function(){
var _8=dc.collectStackedStats(this.series),t;
this._maxRunLength=_8.hmax;
_8.hmin-=0.5;
_8.hmax+=0.5;
t=_8.hmin,_8.hmin=_8.vmin,_8.vmin=t;
t=_8.hmax,_8.hmax=_8.vmax,_8.vmax=t;
return _8;
},render:function(_9,_a){
if(this._maxRunLength<=0){
return this;
}
var _b=df.repeat(this._maxRunLength,"-> 0",0);
for(var i=0;i<this.series.length;++i){
var _c=this.series[i];
for(var j=0;j<_c.data.length;++j){
var _d=_c.data[j];
if(_d!==null){
var v=typeof _d=="number"?_d:_d.y;
if(isNaN(v)){
v=0;
}
_b[j]+=v;
}
}
}
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(_9,_a);
}
this.resetEvents();
this.dirty=this.isDirty();
if(this.dirty){
_2.forEach(this.series,_7);
this._eventSeries={};
this.cleanGroup();
var s=this.group;
df.forEachRev(this.series,function(_e){
_e.cleanGroup(s);
});
}
var t=this.chart.theme,f,_f,_10,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler),_11=this.events();
f=dc.calculateBarSize(this._vScaler.bounds.scale,this.opt);
_f=f.gap;
_10=f.size;
for(var i=this.series.length-1;i>=0;--i){
var _c=this.series[i];
if(!this.dirty&&!_c.dirty){
t.skip();
this._reconnectEvents(_c.name);
continue;
}
_c.cleanGroup();
var _12=t.next("bar",[this.opt,_c]),s=_c.group,_13=new Array(_b.length);
for(var j=0;j<_b.length;++j){
var _d=_c.data[j];
if(_d!==null){
var v=_b[j],_14=ht(v),_15=typeof _d!="number"?t.addMixin(_12,"bar",_d,true):t.post(_12,"bar");
if(_14>=0&&_10>=1){
var _16={x:_a.l,y:_9.height-_a.b-vt(j+1.5)+_f,width:_14,height:_10};
var _17=this._plotFill(_15.series.fill,_9,_a);
_17=this._shapeFill(_17,_16);
var _18=s.createRect(_16).setFill(_17).setStroke(_15.series.stroke);
_c.dyn.fill=_18.getFill();
_c.dyn.stroke=_18.getStroke();
if(_11){
var o={element:"bar",index:j,run:_c,shape:_18,x:v,y:j+1.5};
this._connectEvents(o);
_13[j]=o;
}
if(this.animate){
this._animateBar(_18,_a.l,-_14);
}
}
}
}
this._eventSeries[_c.name]=_13;
_c.dirty=false;
for(var j=0;j<_c.data.length;++j){
var _d=_c.data[j];
if(_d!==null){
var v=typeof _d=="number"?_d:_d.y;
if(isNaN(v)){
v=0;
}
_b[j]-=v;
}
}
}
this.dirty=false;
return this;
}});
});
