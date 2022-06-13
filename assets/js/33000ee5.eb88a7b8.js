"use strict";(self.webpackChunkmeteor=self.webpackChunkmeteor||[]).push([[362],{3905:function(e,t,a){a.d(t,{Zo:function(){return m},kt:function(){return u}});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function s(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var l=r.createContext({}),c=function(e){var t=r.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):s(s({},t),e)),a},m=function(e){var t=c(e.components);return r.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},p=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,l=e.parentName,m=i(e,["components","mdxType","originalType","parentName"]),p=c(a),u=n,f=p["".concat(l,".").concat(u)]||p[u]||d[u]||o;return a?r.createElement(f,s(s({ref:t},m),{},{components:a})):r.createElement(f,s({ref:t},m))}));function u(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,s=new Array(o);s[0]=p;var i={};for(var l in t)hasOwnProperty.call(t,l)&&(i[l]=t[l]);i.originalType=e,i.mdxType="string"==typeof e?e:n,s[1]=i;for(var c=2;c<o;c++)s[c]=a[c];return r.createElement.apply(null,s)}return r.createElement.apply(null,a)}p.displayName="MDXCreateElement"},2038:function(e,t,a){a.r(t),a.d(t,{contentTitle:function(){return l},default:function(){return p},frontMatter:function(){return i},metadata:function(){return c},toc:function(){return m}});var r=a(7462),n=a(3366),o=(a(7294),a(3905)),s=["components"],i={},l="Meteor Metadata Model",c={unversionedId:"reference/metadata_models",id:"reference/metadata_models",isDocsHomePage:!1,title:"Meteor Metadata Model",description:"We have a set of defined metadata models which define the structure of metadata that meteor will yield.",source:"@site/docs/reference/metadata_models.md",sourceDirName:"reference",slug:"/reference/metadata_models",permalink:"/meteor/docs/reference/metadata_models",editUrl:"https://github.com/odpf/meteor/edit/master/docs/docs/reference/metadata_models.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Configuration",permalink:"/meteor/docs/reference/configuration"},next:{title:"Extractors",permalink:"/meteor/docs/reference/extractors"}},m=[{value:"Usage",id:"usage",children:[]}],d={toc:m};function p(e){var t=e.components,a=(0,n.Z)(e,s);return(0,o.kt)("wrapper",(0,r.Z)({},d,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"meteor-metadata-model"},"Meteor Metadata Model"),(0,o.kt)("p",null,"We have a set of defined metadata models which define the structure of metadata that meteor will yield.\nTo visit the metadata models being used by different extractors please visit ",(0,o.kt)("a",{parentName:"p",href:"/meteor/docs/reference/extractors"},"here"),".\nWe are currently using the following metadata models:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/blob/main/odpf/assets/bucket.proto"},"Bucket"),":\nUsed for metadata being extracted from buckets. Buckets are the basic containers in google cloud services, or Amazon S3, etc that are used fot data storage, and quite popular because of their features of access management, aggregation of usage and services and ease of configurations.\nCurrently, Meteor provides a metadata extractor for the buckets mentioned ",(0,o.kt)("a",{parentName:"p",href:"/meteor/docs/reference/extractors"},"here"))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/blob/main/odpf/assets/dashboard.proto"},"Dashboard"),":\nDashboards are an essential part of data analysis and are used to track, analyze and visualize.\nThese Dashboard metadata model includes some basic fields like ",(0,o.kt)("inlineCode",{parentName:"p"},"urn")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"source"),", etc and a list of ",(0,o.kt)("inlineCode",{parentName:"p"},"Chart"),".\nThere are multiple dashboards that are essential for Data Analysis such as metabase, grafana, tableau, etc.\nPlease refer to the list of Dashboards meteor currently supports ",(0,o.kt)("a",{parentName:"p",href:"/meteor/docs/reference/extractors"},"here"),".")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/blob/main/odpf/assets/chart.proto"},"Chart"),":\nCharts are included in all the Dashboard and are the result of certain queries in a Dashboard.\nInformation about them includes the information of the query and few similar details.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/blob/main/odpf/assets/user.proto"},"User"),":\nThis metadata model is used for defining the output of extraction on Users accounts.\nSome of these sources can be GitHub, Workday, Google Suite, LDAP.\nPlease refer to the list of user meteor currently supports ",(0,o.kt)("a",{parentName:"p",href:"/meteor/docs/reference/extractors"},"here"),".")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/blob/main/odpf/assets/table.proto"},"Table"),":\nThis metadata model is being used by extractors based around ",(0,o.kt)("inlineCode",{parentName:"p"},"databases")," or for the ones that store data in tabular format.\nIt contains various fields that include ",(0,o.kt)("inlineCode",{parentName:"p"},"schema")," of the table and other access related information.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/blob/main/odpf/assets/job.proto"},"Job"),":\nMost of the data is being streamed as queues by kafka or other stack in DE pipeline.\nAnd hence Job is a metadata model built for this purpose."))),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"Proto")," has been used to define these metadata models.\nTo check their implementation please refer ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/proton/tree/main/odpf/assets"},"here"),"."),(0,o.kt)("h2",{id:"usage"},"Usage"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-golang"},'import(\n"github.com/odpf/meteor/models/odpf/assets/v1beta1"\n"github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"\n)\n\nfunc main(){\n    // result is a var of data type of assetsv1beta1.Table one of our metadata model\n    result := &assetsv1beta1.Table{\n        // assigining value to metadata model\n        Urn:  fmt.Sprintf("%s.%s", dbName, tableName),\n        Name: tableName,\n    }\n\n    // using column facet to add metadata info of schema\n\n    var columns []*facetsv1beta1.Column\n    columns = append(columns, &facetsv1beta1.Column{\n            Name:       "column_name",\n            DataType:   "varchar",\n            IsNullable: true,\n            Length:     256,\n        })\n    result.Schema = &facetsv1beta1.Columns{\n        Columns: columns,\n    }\n}\n')))}p.isMDXComponent=!0}}]);