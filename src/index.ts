export default {
  async fetch(request, env, ctx): Promise<Response> {
    return new Response("outage-tracker: viewer worker is up\n");
  },

  async scheduled(event, env, ctx): Promise<void> {
    console.log("outage-tracker: cron fired", event.cron);
  },
} satisfies ExportedHandler<Env>;
