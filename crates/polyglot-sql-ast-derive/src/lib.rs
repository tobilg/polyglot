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
    let immutable = node_visitor(input, false, true);
    let untracked = node_visitor(input, false, false);
    let mutable = node_visitor(input, true, false);
    let serialized_variant_names = if name == "Expression" {
        if let Data::Enum(data) = &input.data {
            let names = data.variants.iter().map(|variant| {
                let name = serde_snake_case(&variant.ident.to_string());
                syn::LitStr::new(&name, variant.ident.span())
            });
            quote! {
                impl #name {
                    pub(crate) const SERIALIZED_VARIANT_NAMES: &'static [&'static str] = &[
                        #(#names),*
                    ];
                }
            }
        } else {
            quote!()
        }
    } else {
        quote!()
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

            fn visit_syntax_untracked<'ast, F, T>(&'ast self, visitor: &mut F, type_visitor: &mut T)
            where
                F: FnMut(&'ast crate::expressions::Expression),
                T: FnMut(&'ast crate::expressions::DataType),
            {
                #untracked
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

        #serialized_variant_names
    }
}

fn node_visitor(input: &DeriveInput, mutable: bool, paths: bool) -> proc_macro2::TokenStream {
    let name = &input.ident;
    match &input.data {
        Data::Struct(data) => visit_fields(&data.fields, mutable, paths),
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let (pattern, body) =
                    visit_variant_fields(name, &variant.ident, &variant.fields, mutable, paths);
                quote!(#pattern => { #body })
            });
            quote!(match self { #(#arms),* })
        }
        Data::Union(_) => quote!(),
    }
}

/// Match serde's `rename_all = "snake_case"` behavior for Rust enum variants.
fn serde_snake_case(name: &str) -> String {
    let mut snake_case = String::with_capacity(name.len());
    for (index, character) in name.chars().enumerate() {
        if index > 0 && character.is_uppercase() {
            snake_case.push('_');
        }
        snake_case.extend(character.to_lowercase());
    }
    snake_case
}

fn visit_fields(fields: &Fields, mutable: bool, paths: bool) -> proc_macro2::TokenStream {
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
                    let visit = immutable_visit(&field.ty, immutable_access, paths);
                    if paths {
                        quote! {
                            path.push(crate::ast_children::ChildPathSegment::Field(#field_name));
                            #visit
                            path.pop();
                        }
                    } else {
                        visit
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
                        let visit = immutable_visit(&field.ty, immutable_access, paths);
                        if paths {
                            quote! {
                                path.push(crate::ast_children::ChildPathSegment::Index(#index));
                                #visit
                                path.pop();
                            }
                        } else {
                            visit
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
    paths: bool,
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
                    let visit = immutable_visit(&field.ty, quote!(#ident), paths);
                    if paths {
                        quote! {
                            path.push(crate::ast_children::ChildPathSegment::Field(#field_name));
                            #visit
                            path.pop();
                        }
                    } else {
                        visit
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
                        let visit = immutable_visit(&field.ty, quote!(#binding), paths);
                        if single_expression_payload || !paths {
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

fn immutable_visit(
    ty: &Type,
    access: proc_macro2::TokenStream,
    paths: bool,
) -> proc_macro2::TokenStream {
    if is_expression(ty) {
        return if paths {
            quote!(visitor(path, #access);)
        } else {
            quote!(visitor(#access);)
        };
    }
    if let Some(inner) = container_inner(ty, "Option") {
        let visit = immutable_visit(inner, quote!(value), paths);
        return quote!(if let Some(value) = (#access).as_ref() { #visit });
    }
    if let Some(inner) = container_inner(ty, "Box") {
        return immutable_visit(inner, quote!((#access).as_ref()), paths);
    }
    if let Some(inner) = container_inner(ty, "Vec") {
        let visit = immutable_visit(inner, quote!(value), paths);
        if !paths {
            return quote!(for value in (#access).iter() { #visit });
        }
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
            let visit = immutable_visit(element, quote!(&(#access).#tuple_index), paths);
            if paths {
                quote! {
                    path.push(crate::ast_children::ChildPathSegment::Index(#index));
                    #visit
                    path.pop();
                }
            } else {
                visit
            }
        });
        return quote!(#(#visits)*);
    }
    if is_scalar(ty) {
        return quote!();
    }
    if paths {
        quote!(crate::ast_children::AstNode::visit_expressions(#access, path, visitor);)
    } else {
        let visit_type = if matches!(ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "DataType"))
        {
            quote!(type_visitor(#access);)
        } else {
            quote!()
        };
        quote! {
            #visit_type
            crate::ast_children::AstNode::visit_syntax_untracked(#access, visitor, type_visitor);
        }
    }
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

#[cfg(test)]
mod tests {
    use super::serde_snake_case;

    #[test]
    fn serde_snake_case_matches_expression_variant_serialization() {
        assert_eq!(serde_snake_case("Literal"), "literal");
        assert_eq!(serde_snake_case("ILike"), "i_like");
        assert_eq!(serde_snake_case("JSONBExists"), "j_s_o_n_b_exists");
        assert_eq!(serde_snake_case("SHA2Digest"), "s_h_a2_digest");
        assert_eq!(
            serde_snake_case("CurrentTimestampLTZ"),
            "current_timestamp_l_t_z"
        );
        assert_eq!(serde_snake_case("PropertyEQ"), "property_e_q");
    }
}
