import { WATCH_EVENT_TYPE } from '../enums/watch';
import { K8sObject } from '../utils/kubernetes.model';

export interface WatchEvent {
  type: WATCH_EVENT_TYPE;
  object: K8sObject;
}
