//! Internal derive used to keep Polyglot's typed AST traversal exhaustive.
//!
//! Fields are visited in declaration order. Standard containers are traversed
//! automatically, while `#[ast(skip)]` excludes derived semantic metadata that
//! is not part of the syntax tree.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, Data, DeriveInput, Field, Fields, GenericArgument, PathArguments, Type,
};

#[proc_macro_derive(AstNode, attributes(ast))]
/// Derive immutable and mutable expression-child visitors for an AST payload.
pub fn derive_ast_node(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_ast_node(&input).into()
}

fn expand_ast_node(input: &DeriveInput) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let immutable = match &input.data {
        Data::Struct(data) => visit_fields(&data.fields, false),
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                let (pattern, body) =
                    visit_variant_fields(name, variant_name, &variant.fields, false);
                quote!(#pattern => { #body })
            });
            quote!(match self { #(#arms),* })
        }
        Data::Union(_) => quote!(),
    };
    let mutable = match &input.data {
        Data::Struct(data) => visit_fields(&data.fields, true),
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                let (pattern, body) =
                    visit_variant_fields(name, variant_name, &variant.fields, true);
                quote!(#pattern => { #body })
            });
            quote!(match self { #(#arms),* })
        }
        Data::Union(_) => quote!(),
    };

    quote! {
        impl crate::ast_children::AstNode for #name {
            fn visit_expressions<'ast, F>(
                &'ast self,
                path: &mut Vec<crate::ast_children::ChildPathSegment>,
                visitor: &mut F,
            )
            where
                F: FnMut(
                    &[crate::ast_children::ChildPathSegment],
                    &'ast crate::expressions::Expression,
                ),
            {
                #immutable
            }

            fn visit_expressions_mut<F>(
                &mut self,
                visitor: &mut F,
            )
            where
                F: FnMut(&mut crate::expressions::Expression),
            {
                #mutable
            }
        }
    }
}

fn visit_fields(fields: &Fields, mutable: bool) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields) => {
            let visits = fields.named.iter().filter_map(|field| {
                if is_skipped(field) {
                    return None;
                }
                let ident = field.ident.as_ref().expect("named field");
                let field_name = ident.to_string();
                let access = quote!(&mut self.#ident);
                let immutable_access = quote!(&self.#ident);
                Some(if mutable {
                    mutable_visit(&field.ty, access)
                } else {
                    let visit = immutable_visit(&field.ty, immutable_access);
                    quote! {
                        path.push(crate::ast_children::ChildPathSegment::Field(#field_name));
                        #visit
                        path.pop();
                    }
                })
            });
            quote!(#(#visits)*)
        }
        Fields::Unnamed(fields) => {
            let visits = fields
                .unnamed
                .iter()
                .enumerate()
                .filter_map(|(index, field)| {
                    if is_skipped(field) {
                        return None;
                    }
                    let index = syn::Index::from(index);
                    let access = quote!(&mut self.#index);
                    let immutable_access = quote!(&self.#index);
                    Some(if mutable {
                        mutable_visit(&field.ty, access)
                    } else {
                        let visit = immutable_visit(&field.ty, immutable_access);
                        quote! {
                            path.push(crate::ast_children::ChildPathSegment::Index(#index));
                            #visit
                            path.pop();
                        }
                    })
                });
            quote!(#(#visits)*)
        }
        Fields::Unit => quote!(),
    }
}

fn visit_variant_fields(
    enum_name: &syn::Ident,
    variant_name: &syn::Ident,
    fields: &Fields,
    mutable: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    match fields {
        Fields::Named(fields) => {
            let bindings: Vec<_> = fields
                .named
                .iter()
                .map(|field| {
                    let ident = field.ident.as_ref().expect("named field");
                    if is_skipped(field) || is_scalar(&field.ty) {
                        quote!(#ident: _)
                    } else {
                        quote!(#ident)
                    }
                })
                .collect();
            let visits = fields.named.iter().filter_map(|field| {
                if is_skipped(field) {
                    return None;
                }
                let ident = field.ident.as_ref().expect("named field");
                let field_name = ident.to_string();
                Some(if mutable {
                    mutable_visit(&field.ty, quote!(#ident))
                } else {
                    let visit = immutable_visit(&field.ty, quote!(#ident));
                    quote! {
                        path.push(crate::ast_children::ChildPathSegment::Field(#field_name));
                        #visit
                        path.pop();
                    }
                })
            });
            (
                quote!(#enum_name::#variant_name { #(#bindings),* }),
                quote!(#(#visits)*),
            )
        }
        Fields::Unnamed(fields) => {
            let bindings: Vec<_> = (0..fields.unnamed.len())
                .map(|index| format_ident!("field_{index}"))
                .collect();
            let single_expression_payload = enum_name == "Expression" && fields.unnamed.len() == 1;
            let visits = fields
                .unnamed
                .iter()
                .zip(bindings.iter())
                .enumerate()
                .filter_map(|(index, (field, binding))| {
                    if is_skipped(field) {
                        return None;
                    }
                    Some(if mutable {
                        mutable_visit(&field.ty, quote!(#binding))
                    } else {
                        let visit = immutable_visit(&field.ty, quote!(#binding));
                        if single_expression_payload {
                            visit
                        } else {
                            quote! {
                                path.push(crate::ast_children::ChildPathSegment::Index(#index));
                                #visit
                                path.pop();
                            }
                        }
                    })
                });
            (
                quote!(#enum_name::#variant_name(#(#bindings),*)),
                quote!(#(#visits)*),
            )
        }
        Fields::Unit => (quote!(#enum_name::#variant_name), quote!()),
    }
}

fn immutable_visit(ty: &Type, access: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    if is_expression(ty) {
        return quote!(visitor(path, #access););
    }
    if let Some(inner) = container_inner(ty, "Option") {
        let visit = immutable_visit(inner, quote!(value));
        return quote!(if let Some(value) = (#access).as_ref() { #visit });
    }
    if let Some(inner) = container_inner(ty, "Box") {
        return immutable_visit(inner, quote!((#access).as_ref()));
    }
    if let Some(inner) = container_inner(ty, "Vec") {
        let visit = immutable_visit(inner, quote!(value));
        return quote! {
            for (index, value) in (#access).iter().enumerate() {
                path.push(crate::ast_children::ChildPathSegment::Index(index));
                #visit
                path.pop();
            }
        };
    }
    if let Type::Tuple(tuple) = ty {
        let visits = tuple.elems.iter().enumerate().map(|(index, element)| {
            let tuple_index = syn::Index::from(index);
            let visit = immutable_visit(element, quote!(&(#access).#tuple_index));
            quote! {
                path.push(crate::ast_children::ChildPathSegment::Index(#index));
                #visit
                path.pop();
            }
        });
        return quote!(#(#visits)*);
    }
    if is_scalar(ty) {
        return quote!();
    }
    quote!(crate::ast_children::AstNode::visit_expressions(#access, path, visitor);)
}

fn mutable_visit(ty: &Type, access: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    if is_expression(ty) {
        return quote!(visitor(#access););
    }
    if let Some(inner) = container_inner(ty, "Option") {
        let visit = mutable_visit(inner, quote!(value));
        return quote!(if let Some(value) = (#access).as_mut() { #visit });
    }
    if let Some(inner) = container_inner(ty, "Box") {
        return mutable_visit(inner, quote!((#access).as_mut()));
    }
    if let Some(inner) = container_inner(ty, "Vec") {
        let visit = mutable_visit(inner, quote!(value));
        return quote!(for value in (#access).iter_mut() { #visit });
    }
    if let Type::Tuple(tuple) = ty {
        let visits = tuple.elems.iter().enumerate().map(|(index, element)| {
            let tuple_index = syn::Index::from(index);
            mutable_visit(element, quote!(&mut (#access).#tuple_index))
        });
        return quote!(#(#visits)*);
    }
    if is_scalar(ty) {
        return quote!();
    }
    quote!(crate::ast_children::AstNode::visit_expressions_mut(#access, visitor);)
}

fn container_inner<'a>(ty: &'a Type, expected: &str) -> Option<&'a Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != expected {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn is_expression(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Expression"))
}

fn is_scalar(ty: &Type) -> bool {
    let Type::Path(path) = ty else { return false };
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    matches!(
        segment.ident.to_string().as_str(),
        "bool"
            | "char"
            | "str"
            | "String"
            | "usize"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "f32"
            | "f64"
            | "Span"
    )
}

fn is_skipped(field: &Field) -> bool {
    field.attrs.iter().any(|attribute| {
        attribute.path().is_ident("ast")
            && attribute
                .parse_args::<syn::Ident>()
                .is_ok_and(|ident| ident == "skip")
    })
}
