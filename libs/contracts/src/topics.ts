
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',

  ACTIVATION_REQUESTED_DLQ: 'activation.requested.dlq',
  BILLING_EVENTS_DLQ: 'billing.events.dlq',
  PROVISIONING_EVENTS_DLQ: 'provisioning.events.dlq',
  ACTIVATION_EVENTS_DLQ: 'activation.events.dlq',
} as const;
