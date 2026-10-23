namespace Relaybox.Samples.Imports.Infrastructure;

/// <summary>Sets the tenant of the request from the <c>X-Tenant-Id</c> header. Unknown or missing tenant: 400.</summary>
public sealed class TenantHeaderMiddleware(RequestDelegate next)
{
    public const string Header = "X-Tenant-Id";

    public async Task InvokeAsync(HttpContext context, ITenantRegistry registry, ITenantContext tenantContext)
    {
        var tenantId = context.Request.Headers[Header].ToString();
        if (string.IsNullOrWhiteSpace(tenantId) || await registry.FindByIdAsync(tenantId, context.RequestAborted) is null)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = $"A valid {Header} header is required." });
            return;
        }

        tenantContext.Set(tenantId);
        await next(context);
    }
}
