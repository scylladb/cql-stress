use proc_macro::TokenStream;
use quote::quote_spanned;
use syn::{parse_macro_input, spanned::Spanned, Item};

/// Implements Operation for a type which implements an execute method.
/// Although we could put execute() into the Operation trait, doing what we
/// are doing here has better performance because asynchronous traits require
/// putting returned futures in a Box due to current language limitations.
/// Boxing the futures imply an allocation per operation and those allocations
/// can be clearly visible on the flamegraphs.
#[proc_macro_derive(Operation)]
pub fn runnable(input: TokenStream) -> TokenStream {
    let input_clone = input.clone();
    let item = parse_macro_input!(input_clone as Item);
    let runnable_ty = match &item {
        Item::Struct(s) => &s.ident,
        Item::Enum(e) => &e.ident,
        _ => panic!("Nonsupported place for [runnable] macro. Put it above struct/enum definition."),
    };

    let run_method = quote_spanned!(item.span()=>
        #[async_trait::async_trait]
        impl cql_stress::configuration::Operation for #runnable_ty {
            async fn run(&mut self, mut session: cql_stress::run::WorkerSession) -> anyhow::Result<()> {
                while let Some(ctx) = session.start().await {
                    let result = self.execute(&ctx).await;
                    if let std::ops::ControlFlow::Break(_) = session.end(result)? {
                        return Ok(());
                    }
                }
                Ok(())
            }
        }
    );
    // input.extend(TokenStream::from(run_method));
    // input
    TokenStream::from(run_method)
}
