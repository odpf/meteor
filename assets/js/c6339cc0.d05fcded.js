"use strict";(self.webpackChunkmeteor=self.webpackChunkmeteor||[]).push([[36],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return d}});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function c(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var u=n.createContext({}),f=function(e){var t=n.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=f(e.components);return n.createElement(u.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},l=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,u=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),l=f(r),d=o,m=l["".concat(u,".").concat(d)]||l[d]||s[d]||a;return r?n.createElement(m,i(i({ref:t},p),{},{components:r})):n.createElement(m,i({ref:t},p))}));function d(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,i=new Array(a);i[0]=l;var c={};for(var u in t)hasOwnProperty.call(t,u)&&(c[u]=t[u]);c.originalType=e,c.mdxType="string"==typeof e?e:o,i[1]=c;for(var f=2;f<a;f++)i[f]=r[f];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}l.displayName="MDXCreateElement"},9596:function(e,t,r){r.r(t),r.d(t,{contentTitle:function(){return u},default:function(){return l},frontMatter:function(){return c},metadata:function(){return f},toc:function(){return p}});var n=r(7462),o=r(3366),a=(r(7294),r(3905)),i=["components"],c={},u="Source",f={unversionedId:"concepts/source",id:"concepts/source",isDocsHomePage:!1,title:"Source",description:"When the source field is defined, Meteor will extract data from a metadata source using the details defined in the field. type field should define the name of Extractor you want, you can use one from this list here. config of an extractor can be different for different Extractor and needs you to provide details to set up a connection between meteor and your source. To determine the required configurations you can visit README of each Extractor here.",source:"@site/docs/concepts/source.md",sourceDirName:"concepts",slug:"/concepts/source",permalink:"/meteor/docs/concepts/source",editUrl:"https://github.com/odpf/meteor/edit/master/docs/docs/concepts/source.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Recipe",permalink:"/meteor/docs/concepts/recipe"},next:{title:"Processor",permalink:"/meteor/docs/concepts/processor"}},p=[{value:"Writing source part of your recipe",id:"writing-source-part-of-your-recipe",children:[]}],s={toc:p};function l(e){var t=e.components,r=(0,o.Z)(e,i);return(0,a.kt)("wrapper",(0,n.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"source"},"Source"),(0,a.kt)("p",null,"When the source field is defined, Meteor will extract data from a metadata source using the details defined in the field. ",(0,a.kt)("inlineCode",{parentName:"p"},"type")," field should define the name of Extractor you want, you can use one from this list ",(0,a.kt)("a",{parentName:"p",href:"/meteor/docs/reference/extractors"},"here"),". ",(0,a.kt)("inlineCode",{parentName:"p"},"config")," of an extractor can be different for different Extractor and needs you to provide details to set up a connection between meteor and your source. To determine the required configurations you can visit README of each Extractor ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/odpf/meteor/tree/cb12c3ecf8904cf3f4ce365ca8981ccd132f35d0/plugins/extractors/README.md"},"here"),"."),(0,a.kt)("h2",{id:"writing-source-part-of-your-recipe"},"Writing source part of your recipe"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"source:\n  name: kafka\n  config:\n    broker: broker:9092\n")),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:"left"},"key"),(0,a.kt)("th",{parentName:"tr",align:"left"},"Description"),(0,a.kt)("th",{parentName:"tr",align:"left"},"requirement"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:"left"},(0,a.kt)("inlineCode",{parentName:"td"},"type")),(0,a.kt)("td",{parentName:"tr",align:"left"},"contains the name of extractor, will be used for registry"),(0,a.kt)("td",{parentName:"tr",align:"left"},"required")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:"left"},(0,a.kt)("inlineCode",{parentName:"td"},"config")),(0,a.kt)("td",{parentName:"tr",align:"left"},"different extractor will require different configuration"),(0,a.kt)("td",{parentName:"tr",align:"left"},"optional, depends on extractor")))),(0,a.kt)("p",null,"To get more information about the list of extractors we have, and how to define ",(0,a.kt)("inlineCode",{parentName:"p"},"type")," field refer ",(0,a.kt)("a",{parentName:"p",href:"/meteor/docs/reference/extractors"},"here"),"."))}l.isMDXComponent=!0}}]);